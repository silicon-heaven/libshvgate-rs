use std::path::PathBuf;

use shvclient::shvproto;


pub(crate) struct JournalConfig {
    pub(crate) root_path: PathBuf,
    // TODO: limits
}

pub(crate) struct ShvJournal {
    pub(crate) config: JournalConfig,
}

impl ShvJournal {
    pub(crate) fn new(config: JournalConfig) -> Self {
        Self { config }
    }

    pub(crate) async fn append(&self, path: impl AsRef<str>, value: shvproto::RpcValue) {
        // TODO
    }
}
