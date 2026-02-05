use std::sync::Arc;

use shvclient::clientnode::RpcError;
use shvclient::shvproto::RpcValue;
use shvclient::shvrpc::client::ClientConfig;
use shvclient::{ClientCommandSender, ClientEventsReceiver};

use self::data::{JournalConfig, GateData};
use self::rpc::{RpcHandler, rpc_handler};
use self::tree::{ShvTree, ShvTreeDefinition};

use shvclient::shvrpc::RpcMessage;
pub(crate) use shvclient::shvrpc::Result as ShvRpcResult;

mod data;
mod tree;
mod rpc;

pub(crate) fn send_rpc_signal(
    client_cmd_tx: &ClientCommandSender,
    path: impl AsRef<str>,
    method: impl AsRef<str>,
    signal: impl AsRef<str>,
    value: RpcValue
) -> ShvRpcResult<()>
{
    let (path, method, signal) = (path.as_ref(), method.as_ref(), signal.as_ref());
    client_cmd_tx
        .send_message(RpcMessage::new_signal_with_source(path, signal, method, Some(value)))
        .map_err(|err| format!("Cannot send `{path}:{method}:{signal}` notification: {err}").into())
}

pub struct ShvGate {
    data: Arc<GateData>,
    app_rpc_handler: Option<RpcHandler>,
}


pub struct ShvGateConfig {
    tree: ShvTreeDefinition,
    journal: JournalConfig,
}

impl ShvGate {
    pub async fn new(config: ShvGateConfig) -> Self {
        Self {
            data: Arc::new(GateData::new(config.journal, ShvTree::from_definition(config.tree)).await.unwrap()),
            app_rpc_handler: None,
        }
    }

    pub fn with_method_call_handler<F, Fut, Ret>(mut self, handler: F) -> Self
        where
            F: Fn(String, String, Option<RpcValue>, ClientCommandSender, Arc<GateData>) -> Fut + Sync + Send + 'static,
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
        H: FnOnce(ClientCommandSender, ClientEventsReceiver, Arc<GateData>)
    {
        let rpc_handler = {
            let gate_data = self.data.clone();
            let app_rpc_handler = self.app_rpc_handler.clone();
            move |rq, cmd_sender|
                rpc_handler(rq, cmd_sender, gate_data.clone(), app_rpc_handler.clone())
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
pub(crate) fn init_logger() {
    use std::sync::Once;

    static INIT: Once = Once::new();
    INIT.call_once(|| {
        use simple_logger::SimpleLogger;

        SimpleLogger::new()
            .with_level(log::LevelFilter::Debug)
            .init()
            .unwrap();
        });
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use futures::StreamExt;
    use shvclient::clientapi::RpcCallLsList;
    use shvclient::clientnode::{META_METHOD_GET, META_METHOD_SET, METH_SET};
    use shvrpc::metamethod::{AccessLevel, Flags, MetaMethod};
    use shvrpc::rpcmessage::RpcErrorCode;
    use url::Url;

    use crate::rpc::log_err;

    use super::*;

    fn gate_config() -> ShvGateConfig {
        const META_METHOD_SIM_SET: MetaMethod = MetaMethod::new_static(
            "simSet",
            Flags::UserIDRequired,
            AccessLevel::Command,
            "",
            "",
            &[],
            "",
        );
        ShvGateConfig {
            tree: ShvTreeDefinition {
                nodes_description: BTreeMap::from([
                            ("devices/detectors/TC1/status".into(), tree::NodeDescription {
                                methods: vec![META_METHOD_GET, META_METHOD_SET],
                                sample_type: tree::SampleType::Continuos
                            }),
                            ("devices/detectors/TC2/status".into(), tree::NodeDescription {
                                methods: vec![META_METHOD_GET, META_METHOD_SET, META_METHOD_SIM_SET],
                                sample_type: tree::SampleType::Discrete
                            }),
            ])
            },
            journal: JournalConfig {
                root_path: "journal".into(),
                max_file_entries: 10_000,
                max_journal_size: 10_000_000,
            }
        }
    }

    #[tokio::test]
    async fn it_works() {
        init_logger();

        struct AppState(i32);
        let state = AppState(32);
        let client_config = shvclient::shvrpc::client::ClientConfig {
            url: Url::parse(&std::env::var("BROKER_URL").expect("BROKER_URL env variable should be defined")).unwrap(),
            ..Default::default()
        };
        ShvGate::new(gate_config()).await
            .with_method_call_handler(move |path, method, value, client_cmd_tx, gate_data| async move {
                println!("method call on {path}:{method}, param: {value:?}, state: {}", state.0);
                if method == METH_SET || method == "simSet" {
                    return gate_data
                        .update_value(&path, METH_GET, value.unwrap_or_else(RpcValue::null), false, &client_cmd_tx)
                        .await
                        .map(RpcValue::from)
                        .map_err(|err| {
                            log_err(err);
                            RpcError::new(RpcErrorCode::InternalError, "Cannot update node value")
                        })
                }
                Ok("not implemented".into())
            })
            .run(&client_config, |ccs, mut cer, _gate_data| {
                shvclient::runtime::spawn_task(async move {
                    loop {
                        match cer.next().await {
                            Some(event) => {
                                if matches!(event, shvclient::ClientEvent::Connected(_)) {
                                    break;
                                } else {
                                    panic!("Connection error");
                                }
                            }
                            None => {
                                panic!("Client gone");
                            }
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
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    }
}
