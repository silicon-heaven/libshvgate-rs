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
use shvrpc::{RpcMessage, RpcMessageMetaTags as _};
use tokio::fs::File;
use tokio::sync::RwLock;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

use crate::send_rpc_signal;
use crate::tree::{ShvTree, ShvTreeDefinition};

pub(crate) struct JournalConfig {
    pub(crate) root_path: PathBuf,
    pub(crate) max_file_entries: u64,
    pub(crate) max_journal_size: u64,
}

type FileJournalWriterLog2 = JournalWriterLog2<BufWriter<Compat<File>>>;

struct JournalData {
    log_writer: FileJournalWriterLog2,
    snapshot_keys: BTreeSet<String>,
    entries_count: u64,
}

pub struct GateData {
    pub(crate) start_time: std::time::Instant,
    pub(crate) journal_config: JournalConfig,
    tree: ShvTree,
    journal_data: RwLock<JournalData>,
}

fn journalentry_to_rpcvalue(entry: JournalEntry) -> RpcValue {
    let mut value_flags = ValueFlags::empty();
    if !entry.repeat {
        value_flags.insert(ValueFlags::SPONTANEOUS);
    }
    if entry.provisional {
        value_flags.insert(ValueFlags::PROVISIONAL);
    }

    let data = shvclient::shvproto::make_map!(
        "timestamp" => DateTime::from_epoch_msec(entry.epoch_msec),
        "epochMsec" => entry.epoch_msec,
        "path" => entry.path,
        "value" => entry.value,
        "domain" => entry.signal,
        "valueFlags" => value_flags.bits(),
        "userId" => entry.user_id,
    );
    let mut meta = shvclient::shvproto::MetaMap::new();
    meta.insert(shvrpc::rpctype::Tag::MetaTypeId as i32, (shvrpc::rpctype::global_ns::MetaTypeID::ShvJournalEntry as i32).into());
    RpcValue::new(data.into(), Some(meta))
}

const CMDLOG: &str = "cmdlog";

pub(crate) fn rpc_command_to_journal_entry(rq: &RpcMessage) -> JournalEntry {
    let user_id = rq
        .tag(shvrpc::rpcmessage::Tag::UserId as i32)
        .map(RpcValue::to_cpon);
    let now = DateTime::now();
    let value = format!("{method}({param})",
        method = rq.method().to_owned().unwrap_or_default(),
        param = rq.param().cloned().unwrap_or_else(RpcValue::null).to_cpon()
    );
    JournalEntry {
        path: rq.shv_path().unwrap_or_default().into(),
        signal: CMDLOG.into(),
        source: Default::default(),
        value: value.into(),
        user_id,
        access_level: AccessLevel::Command as _,
        repeat: false,
        provisional: false,
        epoch_msec: now.epoch_msec(),
        short_time: -1,
    }
}

impl GateData {
    pub(crate) async fn new(journal_config: JournalConfig, tree: ShvTree) -> shvrpc::Result<Self> {
        let log_writer = create_log_writer(&journal_config.root_path, &DateTime::now()).await?;
        let snapshot_keys = tree.values.keys().cloned().collect();
        Ok(Self {
            start_time: std::time::Instant::now(),
            tree,
            journal_config,
            journal_data: RwLock::new(JournalData { log_writer, snapshot_keys, entries_count: 0 }),
        })
    }


    pub async fn log_command(&self, cmd_rq: &RpcMessage, client_cmd_tx: &ClientCommandSender) -> shvrpc::Result<()> {
        let journal_entry = rpc_command_to_journal_entry(cmd_rq);
        self.journal_append(&journal_entry)
            .await
            .map_err(|err| format!("Cannot write a command request to the journal, request: `{rq}`, error: {err}", rq = cmd_rq.to_cpon()))?;
        send_rpc_signal(client_cmd_tx, journal_entry.path.clone(), CMDLOG, journalentry_to_rpcvalue(journal_entry))
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

    async fn journal_append(&self, entry: &JournalEntry) -> std::io::Result<()> {
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
        append_entry(journal_data).await?;

        trim_journal(&self.journal_config, journal_data).await
    }


    pub fn read_value(&self, path: impl AsRef<str>) -> Option<RpcValue> {
        self.tree.read(path)
    }

    pub fn tree_definition(&self) -> &ShvTreeDefinition {
        &self.tree.definition
    }
}

async fn trim_journal(journal_config: &JournalConfig, _: &mut JournalData) -> std::io::Result<()> {
    trim_dir(
        &journal_config.root_path,
        journal_config.max_journal_size,
        |path , metadata| metadata.is_file()
        && path.extension().is_some_and(|ext| ext == "log2")
    ).await
}

async fn trim_dir(
    dir_path: impl AsRef<Path>,
    size_limit: u64,
    file_filter: impl Fn(&Path, &std::fs::Metadata) -> bool
) -> std::io::Result<()>
{
    use tokio::fs;
    let mut entries = fs::read_dir(&dir_path).await?;
    let mut overall_size = 0;
    let mut files = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        let metadata = entry.metadata().await?;
        if file_filter(&path, &metadata) {
            let size = metadata.len();
            files.push((path, size));
            overall_size += size;
        }
    }
    files.sort_by(|(path_a,_), (path_b,_)| path_a.cmp(path_b));
    // Keep at least 2 most recent files for log snapshots
    let removable_files = &files[..files.len().saturating_sub(2)];
    for (file_path, file_size) in removable_files {
        if overall_size <= size_limit {
            break
        }
        if let Err(err) = fs::remove_file(file_path).await {
            log::error!("Cannot delete {file_path}: {err}", file_path = file_path.to_string_lossy());
            continue;
        }
        overall_size -= file_size;
        log::info!("Deleted: {path} ({file_size} bytes)", path = file_path.to_string_lossy());
    }
    Ok(())
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
