use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use futures::future::BoxFuture;
use shvclient::ClientCommandSender;
use shvclient::appnodes::DOT_APP_METHODS;
use shvclient::clientnode::{AccessLevel, METH_GET, METH_SET, MetaMethod, Method, RequestHandlerResult, RpcError, StaticNode as _, err_unresolved_request};
use shvclient::shvproto::RpcValue;
use shvclient::shvrpc::util::children_on_path;
use shvclient::shvrpc::{RpcMessage, RpcMessageMetaTags as _};

#[derive(Debug)]
pub(crate) struct ShvTree {
    pub(crate) values: BTreeMap<String, RwLock<RpcValue>>,
    pub(crate) definition: ShvTreeDefinition,
}

#[derive(Debug, Clone)]
pub(crate) struct ShvTreeDefinition {
    pub(crate) node_descriptions: BTreeMap<String, NodeDescription>,
}

impl ShvTreeDefinition {
    pub fn from_yaml(input: &str) -> Self {
        // TODO
        Self { node_descriptions: Default::default() }
    }

    pub fn from_typeinfo(input: &str) -> Self {
        // TODO
        Self { node_descriptions: Default::default() }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SampleType {
    #[default]
    Continuos,
    Discrete,
}

#[derive(Debug, Clone)]
pub(crate) struct NodeDescription {
    pub(crate) methods: Vec<MetaMethod>,
    pub(crate) sample_type: SampleType,
}

impl ShvTree {
    pub(crate) fn from_definition(definition: ShvTreeDefinition) -> Self {
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

    pub(crate) fn update(&self, path: &str, new_value: &RpcValue) -> Option<bool> {
        let Some(locked_value) = self.values.get(path) else {
            return None;
        };
        let mut value = locked_value.write().unwrap();
        if &*value != new_value ||
            self.definition.node_descriptions
                .get(path)
                .is_some_and(|descr| descr.sample_type == SampleType::Discrete)
        {
            *value = new_value.clone();
            return Some(true);
        }
        Some(false)
    }

    pub(crate) fn read(&self, path: &str) -> Option<RpcValue> {
        self.values
            .get(path)
            .map(RwLock::read)
            .map(Result::unwrap)
            .as_deref()
            .map(Clone::clone)
    }
}

// Move to data or rpc module
pub(crate) async fn rpc_handler(
    rq: RpcMessage,
    client_cmd_tx: ClientCommandSender,
    gate_data: Arc<crate::GateData>,
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
                        nodes.append(&mut children_on_path(&gate_data.tree().definition.node_descriptions, ROOT_PATH).unwrap_or_default());
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
            let Some(children) = children_on_path(&gate_data.tree().definition.node_descriptions, &path) else {
                return err_unresolved_request()
            };
            let methods = match gate_data.tree().definition.node_descriptions.get(&path) {
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
                            METH_GET => m.resolve(methods, async move || { Ok(gate_data.tree().read(&path).unwrap_or_else(RpcValue::null)) }),
                            METH_SET => m.resolve(methods, async move || {
                                // Update the value in the tree
                                let res = gate_data.update_value(&path, param.unwrap_or_else(RpcValue::null), &client_cmd_tx).await;
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
pub type RpcHandler = Arc<dyn Fn(String, String, Option<RpcValue>, ClientCommandSender, Arc<crate::GateData>) -> RpcResultFuture + Send + Sync>;
