use std::sync::Arc;

use shvclient::clientnode::RpcError;
use shvclient::shvproto::RpcValue;
use shvclient::shvrpc::client::ClientConfig;
use shvclient::{ClientCommandSender, ClientEventsReceiver};
use shvrpc::RpcMessageMetaTags;

use self::data::GateData;
use self::rpc::{RpcHandler, rpc_handler};
use self::tree::ShvTree;

use shvclient::shvrpc::RpcMessage;
pub(crate) use shvclient::shvrpc::Result as ShvRpcResult;

pub use self::tree::ShvTreeDefinition;
pub use self::data::JournalConfig;

mod data;
mod tree;
mod rpc;
mod fs;

pub(crate) fn send_rpc_signal(
    client_cmd_tx: &ClientCommandSender,
    path: impl AsRef<str>,
    method: impl AsRef<str>,
    signal: impl AsRef<str>,
    value: RpcValue,
    repeat: bool,
) -> ShvRpcResult<()>
{
    let (path, method, signal) = (path.as_ref(), method.as_ref(), signal.as_ref());
    let mut msg = RpcMessage::new_signal_with_source(path, signal, method).with_param(value);
    if repeat {
        msg.set_tag(shvrpc::rpcmessage::Tag::Repeat as i32, Some(true));
    }
    client_cmd_tx
        .send_message(msg)
        .map_err(|err| format!("Cannot send `{path}:{method}:{signal}` notification: {err}").into())
}

pub struct ShvGate {
    data: Arc<GateData>,
    app_rpc_handler: Option<RpcHandler>,
}


pub struct ShvGateConfig {
    pub tree: ShvTreeDefinition,
    pub journal: JournalConfig,
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
