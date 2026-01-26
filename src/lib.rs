use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use futures::future::BoxFuture;

use shvclient::appnodes::DOT_APP_METHODS;
use shvclient::clientnode::{AccessLevel, META_METHOD_GET, META_METHOD_SET, METH_GET, METH_SET, MetaMethod, MetaMethods, Method, RequestHandlerResult, StaticNode as _, err_unresolved_request};
use shvclient::{ClientCommandSender, ClientEventsReceiver, shvproto, shvrpc};
use shvproto::RpcValue;
use shvrpc::rpcmessage::{RpcError, RpcMessageMetaTags};

pub struct ShvGate {
    data: Arc<ShvGateData>,
    // method_call_handler: Option<Arc<dyn MethodCallHandler>>,
    app_rpc_handler: Option<RpcHandler>,
}

pub struct ShvGateData {
    tree: ShvTree,
    journal: ShvJournal,
}

impl ShvGateData {
    pub fn update_value(&self, path: &str, new_value: RpcValue) -> Option<bool> {
        let res = self.tree.update(path, new_value);
        match res {
            None => log::error!("Cannot update value, wrong path `{path}`"),
            Some(val) => if val {
                // TODO: update journal
            },
        }
        res
    }
}

struct JournalConfig {
    root_path: PathBuf,
    // TODO: limits
}

struct ShvJournal {
    config: JournalConfig,
}

impl ShvJournal {
    pub fn new(config: JournalConfig) -> Self {
        Self { config }
    }
    // TODO: append
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

    // pub fn method_call_handler<H: MethodCallHandler>(mut self, handler: H) -> Self {
    //     self.method_call_handler = Some(Arc::new(handler));
    //     self
    // }

    pub fn method_call_handler_fn<F, Fut, Ret>(mut self, handler: F) -> Self
        where
            F: Fn(String, String, Option<RpcValue>, ClientCommandSender, Arc<ShvGateData>) -> Fut + Sync + Send + 'static,
            Fut: Future<Output = Result<Ret, RpcError>> + Send + 'static,
            Ret: Into<RpcValue> + 'static,
    {
        self.app_rpc_handler = Some(
            Arc::new(move |path, method, param, ccs, data| {
                let fut = handler(path, method, param, ccs, data);
                Box::pin(async move { fut.await.map(Into::into) })
            })
        );
        self
    }

    pub async fn run<H>(self, client_config: &shvrpc::client::ClientConfig, on_client_start: H) -> shvrpc::Result<()>
    where
        H: FnOnce(ClientCommandSender, ClientEventsReceiver, Arc<ShvGateData>)
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

async fn rpc_handler(
    rq: shvrpc::RpcMessage,
    client_cmd_tx: ClientCommandSender,
    gate_data: Arc<ShvGateData>,
    app_rpc_handler: Option<RpcHandler>,
) -> RequestHandlerResult
{
    // Root node methods
    const METH_VERSION: &str = "version";
    const METH_UPTIME: &str = "uptime";

    let path = rq.shv_path().map_or_else(String::new, String::from);
    let method = Method::from_request(&rq);
    // let param = rq.param().map_or_else(RpcValue::null, RpcValue::clone);
    let param = rq.param().map(RpcValue::clone);

    match NodeType::from_path(&path) {
        NodeType::Root => {
            const METHODS: &[MetaMethod] = &[
                MetaMethod::new_static(METH_VERSION, 0, AccessLevel::Read, "Null", "String", &[], ""),
                MetaMethod::new_static(METH_UPTIME, 0, AccessLevel::Read, "Null", "String", &[], ""),
            ];
            match method {
                Method::Dir(dir) => dir.resolve(METHODS),
                Method::Ls(ls) => {
                    let ls_handler = async move || {
                        let mut nodes = vec![
                            ".app".to_string(),
                            "test".to_string(),
                        ];
                        // gate_data.tree.definition.node_descriptions
                        const ROOT_PATH: &str = "";
                        nodes.append(&mut shvrpc::util::children_on_path(&gate_data.tree.definition.node_descriptions, ROOT_PATH).unwrap_or_default());
                        Ok(nodes)
                    };
                    ls.resolve(METHODS, ls_handler)
                }
                Method::Other(m) => match m.method() {
                    METH_VERSION => m.resolve(METHODS, async || Ok(env!("CARGO_PKG_VERSION"))),
                    METH_UPTIME => m.resolve(METHODS, async move || {
                        // Ok(humantime::format_duration(std::time::Duration::from_secs(app_state.start_time.elapsed().as_secs())).to_string())
                        if let Some(handler) = app_rpc_handler {
                            handler(path, METH_UPTIME.into(), param, client_cmd_tx, gate_data.clone()).await
                        } else {
                            Ok("not implemented yet".into())
                        }
                    }),
                    _ => err_unresolved_request(),
                }
            }
        }
        NodeType::DotApp => {
            match method {
                Method::Dir(dir) => dir.resolve(DOT_APP_METHODS),
                Method::Ls(ls) => ls.resolve(DOT_APP_METHODS, async move || { Ok(vec![]) }),
                Method::Other(m) => m.resolve_opt(DOT_APP_METHODS, async move ||
                    DOT_APP_NODE.process_request(rq, client_cmd_tx).await
                ),
            }
        }
        NodeType::Device => {
            let Some(children) = shvrpc::util::children_on_path(&gate_data.tree.definition.node_descriptions, &path) else {
                return err_unresolved_request()
            };
            let methods = match gate_data.tree.definition.node_descriptions.get(&path) {
                Some(descr) => descr.methods.clone(),
                None => Default::default(),
            };
            match method {
                Method::Dir(dir) => dir.resolve(methods),
                Method::Ls(ls) => ls.resolve(methods, async move || { Ok(children) }),
                Method::Other(m) => {
                    let method_name = m.method();
                    if methods.iter().find(|mm| mm.name == method_name).is_some() {
                        // Handle "set" and "get"
                        // TODO
                        match method_name {
                            METH_GET => m.resolve(methods, async move || { Ok(gate_data.tree.read(&path).unwrap_or_else(RpcValue::null)) }),
                            METH_SET => m.resolve(methods, async move || {
                                // Update the value in the tree
                                let res = gate_data.update_value(&path, param.unwrap_or_else(RpcValue::null));
                                    Ok(res.unwrap_or_default())
                            }),
                            _ => {
                                let Some(handler) = app_rpc_handler else {
                                    return err_unresolved_request()
                                };
                                let method_name = method_name.to_string();
                                m.resolve(methods, async move || {
                                    handler(path, method_name, param, client_cmd_tx, gate_data.clone()).await
                                })
                            }
                        }
                    } else {
                        err_unresolved_request()
                    }
                }
            }
        }
    }

}

static DOT_APP_NODE: std::sync::LazyLock<shvclient::appnodes::DotAppNode> = std::sync::LazyLock::new(||
    shvclient::appnodes::DotAppNode::new("shvgate-rs")
);

enum NodeType {
    Root,
    DotApp,
    // ShvJournal,
    Device,
}

impl NodeType {
    fn from_path(path: &str) -> Self {
        match path {
            "" => Self::Root,
            ".app" => Self::DotApp,
            // "_valuecache" => Self::ValueCache,
            // path if path.starts_with("_shvjournal") => Self::ShvJournal,
            _ => Self::Device,
        }
    }
}

pub type RpcResult = Result<RpcValue, RpcError>;
pub type RpcResultFuture = BoxFuture<'static, RpcResult>;
pub type RpcHandler = Arc<dyn Fn(String, String, Option<RpcValue>, ClientCommandSender, Arc<ShvGateData>) -> RpcResultFuture + Send + Sync>;

// pub trait MethodCallHandler: Send + Sync + 'static {
//     fn call(
//         &self,
//         path: String,
//         method: String,
//         param: Option<RpcValue>,
//         ctx: ShvGateContext,
//     ) -> MethodCallFuture;
// }
//
// impl<F, Fut, Ret> MethodCallHandler for F
// where
//     F: Fn(String, String, Option<RpcValue>, ShvGateContext) -> Fut + Sync + Send + 'static,
//     Fut: Future<Output = Result<Ret, RpcError>> + Send + 'static,
//     Ret: Into<RpcValue> + 'static,
// {
//     fn call(
//         &self,
//         path: String,
//         method: String,
//         param: Option<RpcValue>,
//         ctx: ShvGateContext,
//     ) -> MethodCallFuture {
//         let fut = self(path, method, param, ctx);
//         Box::pin(async move { fut.await.map(Into::into) })
//     }
// }

#[derive(Debug)]
pub struct ShvTree {
    values: BTreeMap<String, RwLock<RpcValue>>,
    definition: ShvTreeDefinition,
}

#[derive(Debug, Clone)]
pub struct ShvTreeDefinition {
    node_descriptions: BTreeMap<String, NodeDescription>,
}

impl ShvTreeDefinition {
    pub fn new_test() -> Self {
        Self {
            node_descriptions: BTreeMap::from([
                            ("devices/detectors/TC1/status".into(), NodeDescription { methods: vec![META_METHOD_GET, META_METHOD_SET] }),
                            ("devices/detectors/TC2/status".into(), NodeDescription { methods: vec![] }),
            ])
        }
    }
}

#[derive(Debug, Clone)]
struct NodeDescription {
    methods: Vec<MetaMethod>,
}

impl ShvTree {
    pub fn from_definition(definition: ShvTreeDefinition) -> Self {
        let values = definition
            .node_descriptions
            .iter()
            .filter_map(|(path, descr)| descr
                .methods
                .iter()
                .find(|mm| mm.name == METH_GET)
                .map(|_| (path.clone(), RwLock::new(0.into())))
            )
            .collect();

        Self { values, definition }
    }

    pub fn update(&self, path: &str, new_value: RpcValue) -> Option<bool> {
        let Some(locked_value) = self.values.get(path) else {
            // panic!("Value node on path `{path}` does not exist");
            return None;
        };
        let mut value = locked_value.write().unwrap();
        if *value == new_value {
            return Some(false);
        }
        *value = new_value;
        Some(true)
    }

    pub fn read(&self, path: &str) -> Option<RpcValue> {
        self.values
            .get(path)
            .map(RwLock::read)
            .map(Result::unwrap)
            .as_deref()
            .map(Clone::clone)
    }
}


#[cfg(test)]
mod tests {
    use shvclient::clientapi::{RpcCall, RpcCallLsList};
    use url::Url;

    use super::*;

    #[tokio::test]
    async fn it_works() {
        struct AppState(i32);
        let state = AppState(32);
        let client_config = shvrpc::client::ClientConfig {
            url: Url::parse(&std::env::var("BROKER_URL").expect("BROKER_URL env variable should be defined")).unwrap(),
            ..Default::default()
        };
        let config = ShvGateConfig {
            tree: ShvTreeDefinition::new_test(),
            journal: JournalConfig { root_path: "journal".into() }
        };
        let app_rpc_handler = move |path, method, value, ccs, tree| async move {
            println!("method call on {path}:{method}, param: {value:?}, state: {}", state.0);
            Ok(true)
        };
        ShvGate::new(config)
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
