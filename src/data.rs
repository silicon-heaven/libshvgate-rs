use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use futures::{AsyncWrite, AsyncWriteExt as _};
use futures::io::BufWriter;
use shvclient::ClientCommandSender;
use shvclient::clientnode::SIG_CHNG;
use shvclient::shvproto::{DateTime, IMap, RpcValue, make_imap, make_map};
use shvrpc::datachange::{DataChange, ValueFlags};
use shvrpc::journalentry::JournalEntry;
use shvrpc::journalrw::JournalWriterLog2;
use shvrpc::metamethod::AccessLevel;
use shvrpc::{RpcMessage, RpcMessageMetaTags as _};
use tokio::fs::File;
use tokio::sync::RwLock;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

use crate::send_rpc_signal;
use crate::tree::{CachedValue, PathMethod, ShvTree, ShvTreeDefinition};

pub struct JournalConfig {
    pub root_path: PathBuf,
    pub max_file_entries: u64,
    pub max_journal_size: u64,
}

type FileJournalWriterLog2 = JournalWriterLog2<BufWriter<Compat<File>>>;
type FileJournalWriterLog3 = JournalWriterLog3<BufWriter<Compat<File>>>;

struct JournalData {
    log_writer: FileJournalWriterLog3,
    snapshot_keys: BTreeSet<PathMethod>,
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
    let method = rq.method().to_owned().unwrap_or_default();
    let value = format!("{method}({param})",
        param = rq.param().cloned().unwrap_or_else(RpcValue::null).to_cpon()
    );
    JournalEntry {
        path: rq.shv_path().unwrap_or_default().into(),
        signal: CMDLOG.into(),
        source: method.into(),
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
        let log_writer = create_log3_writer(&journal_config.root_path, &DateTime::now()).await?;
        let snapshot_keys = tree.snapshot_keys().cloned().collect();
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
        send_rpc_signal(client_cmd_tx, journal_entry.path.clone(), journal_entry.source.clone(), CMDLOG, journalentry_to_rpcvalue(journal_entry))
    }

    // TODO: signal parameter, log only *chng signals
    pub async fn update_value(
        &self,
        path: impl AsRef<str>,
        method: impl AsRef<str>,
        new_value: RpcValue,
        force_chng: bool,
        client_cmd_tx: &ClientCommandSender
    ) -> shvrpc::Result<bool>
    {
        let path = path.as_ref();
        let method = method.as_ref();
        match self.tree.update(path, method, &new_value) {
            None => Err(format!("Cannot update value. Cache for method `{path}:{method}` does not exist").into()),
            Some(updated) => {
                if (updated || force_chng) && self.tree.method_has_signal(path, method, SIG_CHNG) {
                    let now = DateTime::now();
                    let data_change = DataChange {
                        date_time: Some(now),
                        value_flags: ValueFlags::SPONTANEOUS,
                        ..DataChange::from(new_value.clone())
                    };
                    let entry = datachange_to_journal_entry(path, method, data_change.clone(), &now);
                    self.journal_append(&entry)
                        .await
                        .map_err(|err| format!("Journal write error: {err}"))?;
                    send_rpc_signal(client_cmd_tx, path, method, SIG_CHNG, data_change.into())?;
                }
                Ok(updated)
            }
        }
    }

    async fn journal_append(&self, entry: &JournalEntry) -> std::io::Result<()> {
        let journal_data = &mut *self.journal_data.write().await;
        let append_entry = async |journal_data: &mut JournalData| {
            journal_data.log_writer.append(entry.clone()).await?;
            journal_data.snapshot_keys.remove(&PathMethod(entry.path.clone(), entry.source.clone()));
            journal_data.entries_count +=1;
            Ok(())
        };
        if journal_data.entries_count < self.journal_config.max_file_entries {
            return append_entry(journal_data).await;
        }
        let snapshot_msec = entry.epoch_msec;
        // Get owned version of snapshot_keys to consume its values while iterating
        let mut remaining_snapshot_keys = std::mem::take(&mut journal_data.snapshot_keys).into_iter();
        while let Some(snapshot_entry_path_method) = remaining_snapshot_keys.next() {
            if let Some(cached_value) = self.tree.cache.get(&snapshot_entry_path_method) {
                let PathMethod(path, method) = &snapshot_entry_path_method;
                let CachedValue(Some(value)) = cached_value
                    .read()
                    .unwrap_or_else(|_| panic!("Poisoned RwLock of tree node {path}:{method}"))
                    .clone() else {
                        continue
                };
                let entry = JournalEntry {
                    epoch_msec: snapshot_msec,
                    path: path.into(),
                    signal: SIG_CHNG.into(),
                    source: method.into(),
                    value,
                    access_level: AccessLevel::Read as _,
                    short_time: -1,
                    user_id: None,
                    repeat: true,
                    provisional: false,
                };
                if let Err(err) = journal_data.log_writer.append(entry).await {
                    // Leave snapshot_keys in consistent state on failure (although the last entry
                    // write state is undefined).
                    journal_data.snapshot_keys.insert(snapshot_entry_path_method);
                    journal_data.snapshot_keys.extend(remaining_snapshot_keys);
                    return Err(err);
                }
            }
        }
        // At this point, journal_data.entries_count >= self.journal_config.max_file_entries holds and
        // journal_data.snapshot_keys is empty. If create_log_writer fails here and then journal_append
        // is called again, it will just proceed here again without any other effects.
        journal_data.log_writer = create_log3_writer(&self.journal_config.root_path, &DateTime::now()).await?;
        journal_data.snapshot_keys = self.tree.snapshot_keys().cloned().collect();
        journal_data.entries_count = 0;

        // Append the journal entry to the new file
        append_entry(journal_data).await?;

        trim_journal(&self.journal_config, "log3", journal_data).await
    }


    pub fn cached_value(&self, path: impl Into<String>, method: impl Into<String>) -> Option<RpcValue> {
        self.tree.read(path, method)
    }

    pub fn tree_definition(&self) -> &ShvTreeDefinition {
        &self.tree.definition
    }
}

async fn trim_journal(journal_config: &JournalConfig, files_ext: &str, _: &mut JournalData) -> std::io::Result<()> {
    trim_dir(
        &journal_config.root_path,
        journal_config.max_journal_size,
        |path , metadata| metadata.is_file()
        && path.extension().is_some_and(|ext| ext == files_ext)
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

fn datachange_to_journal_entry(path: impl Into<String>, method: impl Into<String>, data_change: DataChange, default_date_time: &DateTime) -> JournalEntry {
    JournalEntry {
        epoch_msec: data_change.date_time.as_ref().map_or_else(|| default_date_time.epoch_msec(), |dt| dt.epoch_msec()),
        path: path.into(),
        signal: SIG_CHNG.into(),
        source: method.into(),
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

async fn create_journal_file(base_path: impl AsRef<Path>, file_name: impl AsRef<Path>) -> std::io::Result<File> {
    let path = base_path.as_ref().join(file_name);
    tokio::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .create(true)
        .open(&path)
        .await
        .map_err(|err| std::io::Error::new(
                err.kind(),
                format!("Cannot create journal file `{path}`: {err}", path = path.display())
        ))
}

async fn create_log2_writer(base_path: impl AsRef<Path>, date_time: &DateTime) -> Result<FileJournalWriterLog2, std::io::Error> {
    create_journal_file(base_path, datetime_to_log2_filename(date_time))
        .await
        .map(|file| FileJournalWriterLog2::new(BufWriter::new(file.compat_write())))
}

fn datetime_to_log3_filename(dt: &DateTime) -> String {
    dt
        .to_chrono_datetime()
        .format("%Y-%m-%dT%H-%M-%S.log3")
        .to_string()
}

async fn create_log3_writer(base_path: impl AsRef<Path>, date_time: &DateTime) -> Result<FileJournalWriterLog3, std::io::Error> {
    let file = create_journal_file(base_path, datetime_to_log3_filename(date_time)).await?;
    JournalWriterLog3::new(BufWriter::new(file.compat_write())).await
}

#[repr(i32)]
pub enum Log3IKey {
    Time = 1,
    Path,
    Signal,
    Source,
    Value,
    AccessLevel,
    UserId,
    Repeat,
}


pub fn journal_entry_to_log3_imap<'a>(entry: JournalEntry) -> IMap {
    let time = match entry.epoch_msec {
        ..=0 =>  None,
        val => Some(DateTime::from_epoch_msec(val)),
    };
    make_imap!(
        Log3IKey::Time as _ => time,
        Log3IKey::Path as _ => entry.path,
        Log3IKey::Signal as _ => entry.signal,
        Log3IKey::Source as _ => entry.source,
        Log3IKey::Value as _ => entry.value,
        Log3IKey::AccessLevel as _ => entry.access_level,
        Log3IKey::UserId as _ => entry.user_id,
        Log3IKey::Repeat as _ => entry.repeat,
    )
}

pub struct JournalWriterLog3<W> {
    writer: W,
}

impl<W> JournalWriterLog3<W>
where
    W: AsyncWrite + Unpin,
{
    pub async fn new(mut writer: W) -> std::io::Result<Self> {
        let header = RpcValue::from(make_map!("version" => 3.0)).to_cpon() + "\n";
        Self::write_bytes(&mut writer, header.as_bytes()).await?;
        Ok(Self {
            writer,
        })
    }

    pub async fn append(&mut self, entry: JournalEntry) -> std::io::Result<()> {
        let line = RpcValue::from(journal_entry_to_log3_imap(entry)).to_cpon() + "\n";
        Self::write_bytes(&mut self.writer, line.as_bytes()).await
    }

    async fn write_bytes(writer: &mut W, bytes: &[u8]) -> std::io::Result<()> {
        writer.write_all(bytes).await?;
        writer.flush().await
    }
}
