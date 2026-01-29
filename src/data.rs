use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use futures::io::BufWriter;
use shvclient::ClientCommandSender;
use shvclient::clientnode::{METH_GET, SIG_CHNG};
use shvclient::shvproto::{DateTime, RpcValue};
use shvrpc::datachange::{DataChange, ValueFlags};
use shvrpc::journalentry::JournalEntry;
use shvrpc::journalrw::JournalWriterLog2;
use shvrpc::metamethod::AccessLevel;
use tokio::fs::File;
use tokio::sync::RwLock;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

use crate::send_rpc_signal;
use crate::tree::ShvTree;

pub(crate) struct JournalConfig {
    pub(crate) root_path: PathBuf,
    pub(crate) max_file_entries: u64,
}

type FileJournalWriterLog2 = JournalWriterLog2<BufWriter<Compat<File>>>;

struct JournalData {
    log_writer: FileJournalWriterLog2,
    snapshot_keys: BTreeSet<String>,
    entries_count: u64,
}

pub struct GateData {
    tree: ShvTree,
    journal_config: JournalConfig,
    journal_data: RwLock<JournalData>,
}

impl GateData {
    pub(crate) async fn new(journal_config: JournalConfig, tree: ShvTree) -> shvrpc::Result<Self> {
        let log_writer = create_log_writer(&journal_config.root_path, &DateTime::now()).await?;
        let snapshot_keys = tree.values.keys().cloned().collect();
        Ok(Self {
            tree,
            journal_config,
            journal_data: RwLock::new(JournalData { log_writer, snapshot_keys, entries_count: 0 }),
        })
    }

    pub async fn update_value(&self, path: &str, new_value: RpcValue, client_cmd_tx: &ClientCommandSender) -> shvrpc::Result<bool> {
        match self.tree.update(path, &new_value) {
            None => Err(format!("Value node on path `{path}` does not exist").into()),
            Some(updated) => {
                if updated {
                    let now = DateTime::now();
                    let data_change = DataChange {
                        date_time: Some(now),
                        ..DataChange::from(new_value.clone())
                    };
                    let entry = datachange_to_journal_entry(path, data_change.clone(), &now);
                    self.journal_append(&entry)
                        .await
                        .map_err(|err| format!("Journal write error: {err}"))?;
                    send_rpc_signal(client_cmd_tx, path, SIG_CHNG, data_change.into())?;
                }
                Ok(updated)
            }
        }
    }

    async fn journal_append(&self, entry: &JournalEntry) -> Result<(), std::io::Error> {
        let journal_data = &mut *self.journal_data.write().await;
        let append_entry = async |journal_data: &mut JournalData| {
            journal_data.log_writer.append(entry).await?;
            journal_data.snapshot_keys.remove(&entry.path);
            journal_data.entries_count +=1;
            Ok(())
        };
        if journal_data.entries_count < self.journal_config.max_file_entries {
            return append_entry(journal_data).await;
        }
        let snapshot_msec = entry.epoch_msec;
        // Get owned version of snapshot_keys to consume its values while iterating
        let mut remaining_snapshot_keys = std::mem::take(&mut journal_data.snapshot_keys).into_iter();
        while let Some(snapshot_entry_path) = remaining_snapshot_keys.next() {
            if let Some(node) = self.tree.values.get(&snapshot_entry_path) {
                let node = node.read().unwrap_or_else(|_| panic!("Poisoned RwLock of tree node {snapshot_entry_path}")).clone();
                let entry = JournalEntry {
                    epoch_msec: snapshot_msec,
                    path: snapshot_entry_path,
                    signal: SIG_CHNG.into(),
                    source: METH_GET.into(),
                    value: node,
                    access_level: AccessLevel::Read as _,
                    short_time: -1,
                    user_id: None,
                    repeat: true,
                    provisional: false,
                };
                if let Err(err) = journal_data.log_writer.append(&entry).await {
                    // Leave snapshot_keys in consistent state on failure (although the last entry
                    // write state is undefined).
                    journal_data.snapshot_keys.insert(entry.path);
                    journal_data.snapshot_keys.extend(remaining_snapshot_keys);
                    return Err(err);
                }
            }
        }
        // At this point, journal_data.entries_count >= self.journal_config.max_file_entries holds and
        // journal_data.snapshot_keys is empty. If create_log_writer fails here and then journal_append
        // is called again, it will just proceed here again without any other effects.
        journal_data.log_writer = create_log_writer(&self.journal_config.root_path, &DateTime::now()).await?;
        journal_data.snapshot_keys = self.tree.values.keys().cloned().collect();
        journal_data.entries_count = 0;
        // Append the journal entry to the new file
        append_entry(journal_data).await
    }

    pub fn tree(&self) -> &ShvTree {
        &self.tree
    }
}

// TODO: variant with custom signal, source
fn datachange_to_journal_entry(path: impl Into<String>, data_change: DataChange, default_date_time: &DateTime) -> JournalEntry {
    JournalEntry {
        epoch_msec: data_change.date_time.as_ref().map_or_else(|| default_date_time.epoch_msec(), |dt| dt.epoch_msec()),
        path: path.into(),
        signal: SIG_CHNG.into(),
        source: METH_GET.into(),
        value: data_change.value,
        access_level: AccessLevel::Read as _,
        short_time: data_change.short_time.unwrap_or(-1),
        user_id: None,
        repeat: !data_change.value_flags.contains(ValueFlags::SPONTANEOUS),
        provisional: data_change.value_flags.contains(ValueFlags::PROVISIONAL),
    }
}

fn datetime_to_log2_filename(dt: &DateTime) -> String {
    dt
        .to_chrono_datetime()
        .format("%Y-%m-%dT%H-%M-%S-%3f.log2")
        .to_string()
}

fn msec_to_log2_filename(msec: i64) -> String {
    datetime_to_log2_filename(&DateTime::from_epoch_msec(msec))
}

async fn create_log_writer(base_path: impl AsRef<Path>, date_time: &DateTime) -> Result<FileJournalWriterLog2, std::io::Error> {
    let path = base_path.as_ref().join(datetime_to_log2_filename(date_time));
    tokio::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .create(true)
        .open(&path)
        .await
        .map(|file| FileJournalWriterLog2::new(BufWriter::new(file.compat_write())))
        .map_err(|err| std::io::Error::new(
                err.kind(),
                format!("Cannot create journal file `{path}`: {err}", path = path.to_string_lossy())
        ))
}
