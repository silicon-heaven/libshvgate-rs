use std::sync::Arc;

use shvclient::clientnode::RpcError;
use shvclient::shvproto::RpcValue;
use shvclient::shvrpc::client::ClientConfig;
use shvclient::{ClientCommandSender, ClientEventsReceiver};

use self::journal::{JournalConfig, ShvJournal};
use self::tree::{ShvTree, ShvTreeDefinition};

use shvclient::shvrpc::{Result as ShvRpcResult};

mod journal;
mod tree;

pub struct ShvGate {
    data: Arc<ShvGateData>,
    app_rpc_handler: Option<tree::RpcHandler>,
}

pub struct ShvGateData {
    pub(crate) tree: ShvTree,
    pub(crate) journal: ShvJournal,
}

impl ShvGateData {
    pub async fn update_value(&self, path: &str, new_value: RpcValue) -> Option<bool> {
        let res = self.tree.update(path, &new_value);
        match res {
            None => log::error!("Cannot update value, path `{path}` "),
            Some(updated) => if updated {
                self.journal.append(path, new_value).await;
                // TODO: send notification
            },
        }
        res
    }
}

pub struct ShvGateConfig {
    tree: ShvTreeDefinition,
    journal: JournalConfig,
}

impl ShvGate {
    pub fn new(config: ShvGateConfig) -> Self {
        Self {
            data: Arc::new(ShvGateData {
                tree: ShvTree::from_definition(config.tree),
                journal: ShvJournal::new(config.journal),
            }),
            app_rpc_handler: None,
        }
    }

    pub fn method_call_handler_fn<F, Fut, Ret>(mut self, handler: F) -> Self
        where
            F: Fn(String, String, Option<RpcValue>, ClientCommandSender, Arc<ShvGateData>) -> Fut + Sync + Send + 'static,
            Fut: Future<Output = Result<Ret, RpcError>> + Send + 'static,
            Ret: Into<RpcValue>,
    {
        self.app_rpc_handler = Some(
            Arc::new(move |path, method, param, ccs, data| {
                let fut = handler(path, method, param, ccs, data);
                Box::pin(async move { fut.await.map(Into::into) })
            })
        );
        self
    }

    pub async fn run<H>(self, client_config: &ClientConfig, on_client_start: H) -> ShvRpcResult<()>
    where
        H: FnOnce(ClientCommandSender, ClientEventsReceiver, Arc<ShvGateData>)
    {
        let rpc_handler = {
            let gate_data = self.data.clone();
            let app_rpc_handler = self.app_rpc_handler.clone();
            move |rq, cmd_sender|
                tree::rpc_handler(rq, cmd_sender, gate_data.clone(), app_rpc_handler.clone())
        };
        shvclient::Client::new()
            .mount_dynamic("", rpc_handler)
            .run_with_init(client_config, {
                let gate_data = self.data.clone();
                |client_cmd_tx, client_evt_rx|
                    on_client_start(client_cmd_tx, client_evt_rx, gate_data)
            })
            .await
    }
}


#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use shvclient::clientapi::RpcCallLsList;
    use shvclient::clientnode::{META_METHOD_GET, META_METHOD_SET};
    use url::Url;

    use super::*;

    fn gate_config() -> ShvGateConfig {
        ShvGateConfig {
            tree: ShvTreeDefinition {
                node_descriptions: BTreeMap::from([
                            ("devices/detectors/TC1/status".into(), tree::NodeDescription { methods: vec![META_METHOD_GET, META_METHOD_SET] }),
                            ("devices/detectors/TC2/status".into(), tree::NodeDescription { methods: vec![] }),
            ])
            },
            journal: JournalConfig { root_path: "journal".into() }
        }
    }

    #[tokio::test]
    async fn it_works() {
        struct AppState(i32);
        let state = AppState(32);
        let client_config = shvclient::shvrpc::client::ClientConfig {
            url: Url::parse(&std::env::var("BROKER_URL").expect("BROKER_URL env variable should be defined")).unwrap(),
            ..Default::default()
        };
        let app_rpc_handler = move |path, method, value, ccs, tree| async move {
            println!("method call on {path}:{method}, param: {value:?}, state: {}", state.0);
            Ok(true)
        };
        ShvGate::new(gate_config())
            .method_call_handler_fn(app_rpc_handler)
            .run(&client_config, |ccs, mut cer, gate_data| {
                shvclient::runtime::spawn_task(async move {
                    loop {
                        let event = cer.wait_for_event().await.unwrap();
                        if matches!(event, shvclient::ClientEvent::Connected(_)) {
                            break;
                        }
                    }
                    let ls_res = RpcCallLsList::new(".broker")
                        .exec(&ccs)
                        .await
                        .unwrap();
                    println!("App started: {ls_res:?}");
                }).detach();
            }
            )
            .await
            .unwrap();

    }
}
