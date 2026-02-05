use std::sync::Arc;

use futures::future::BoxFuture;
use shvclient::ClientCommandSender;
use shvclient::appnodes::DOT_APP_METHODS;
use shvclient::clientnode::{Method, RequestHandlerResult, StaticNode, err_unresolved_request};
use shvclient::shvproto::RpcValue;
use shvrpc::metamethod::{AccessLevel, MetaMethod, Flags as MetaMethodFlags};
use shvrpc::rpcmessage::{RpcError, RpcErrorCode};
use shvrpc::util::children_on_path;
use shvrpc::{RpcMessage, RpcMessageMetaTags as _};

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
    const METH_GET_LOG: &str = "getLog";

    let path = rq.shv_path().map_or_else(String::new, String::from);
    let method = Method::from_request(&rq);
    let param = rq.param().map(RpcValue::clone);

    match NodeType::from_path(&path) {
        NodeType::Root => {
            const METHODS: &[MetaMethod] = &[
                MetaMethod::new_static(METH_VERSION, MetaMethodFlags::None, AccessLevel::Read, "Null", "String", &[], ""),
                MetaMethod::new_static(METH_UPTIME, MetaMethodFlags::None, AccessLevel::Read, "Null", "String", &[], ""),
                MetaMethod::new_static(METH_GET_LOG, MetaMethodFlags::None, AccessLevel::Read, "RpcValue", "RpcValue", &[], ""),
            ];
            match method {
                Method::Dir(dir) => dir.resolve(METHODS),
                Method::Ls(ls) => {
                    let ls_handler = async move || {
                        let mut nodes = vec![".app".to_string()];
                        nodes.extend(children_on_path(&gate_data.tree_definition().nodes_description, "").unwrap_or_default());
                        Ok(nodes)
                    };
                    ls.resolve(METHODS, ls_handler)
                }
                Method::Other(m) => match m.method() {
                    METH_VERSION => m.resolve(METHODS, async || Ok(env!("CARGO_PKG_VERSION"))),
                    METH_UPTIME => m.resolve(METHODS, async move || Ok(
                            humantime::format_duration(
                                std::time::Duration::from_secs(gate_data.start_time.elapsed().as_secs())
                            )
                            .to_string())
                    ),
                    METH_GET_LOG => m.resolve(METHODS, async move || getlog_handler(&path, &param, &client_cmd_tx).await),
                    _ => err_unresolved_request(),
                }
            }
        }
        NodeType::DotApp => {
            match method {
                Method::Dir(dir) => dir.resolve(DOT_APP_METHODS),
                Method::Ls(ls) => ls.resolve(DOT_APP_METHODS, async move || Ok(vec![])),
                Method::Other(m) => m.resolve_opt(DOT_APP_METHODS, async move ||
                    DOT_APP_NODE.process_request(rq, client_cmd_tx).await
                ),
            }
        }
        NodeType::Device => {
            let nodes_description = &gate_data.tree_definition().nodes_description;
            let Some(children) = children_on_path(nodes_description, &path) else {
                return err_unresolved_request()
            };
            let methods = match nodes_description.get(&path) {
                Some(descr) => descr.methods.clone(),
                None => Default::default(),
            };
            match method {
                Method::Dir(dir) => dir.resolve(methods),
                Method::Ls(ls) => ls.resolve(methods, async move || Ok(children)),
                Method::Other(m) => {
                    let Some(mm) = methods.iter().find(|mm| mm.name == m.method()) else {
                        return err_unresolved_request()
                    };
                    let is_user_id_required = mm.flags.contains(MetaMethodFlags::UserIDRequired);
                    let log_user_command = async move |gate_data: &Arc<crate::data::GateData>, rq: &RpcMessage, client_cmd_tx: &ClientCommandSender| {
                        if is_user_id_required {
                            // Log commands that require user ID in a request metadata
                            gate_data
                                .log_command(&rq, &client_cmd_tx)
                                .await
                                .unwrap_or_else(log_err);
                        }
                    };
                    let method = m.method().to_owned();
                    if mm.flags.contains(MetaMethodFlags::IsGetter) {
                        m.resolve(methods, async move || {
                            log_user_command(&gate_data, &rq, &client_cmd_tx).await;
                            Ok(gate_data.cached_value(path, method)) //.unwrap_or_else(RpcValue::null))
                        })
                    } else {
                        m.resolve(methods, async move || {
                            log_user_command(&gate_data, &rq, &client_cmd_tx).await;
                            let Some(app_handler) = app_rpc_handler else {
                                return Err(RpcError::new(
                                        RpcErrorCode::NotImplemented,
                                        format!("'{path}:{method}()' is defined but not implemented")
                                ))
                            };
                            app_handler(path, method, param, client_cmd_tx, gate_data).await
                        })
                    }
                }
            }
        }
    }
}

async fn getlog_handler(
    _path: &str,
    _param: &Option<RpcValue>,
    _client_cmd_tx: &ClientCommandSender,
) -> RpcResult {
    // Dummy implementation
    Ok(RpcValue::from("Log is not implemented"))
}

pub(crate) fn log_err(msg: impl std::fmt::Display) {
    log::error!("{msg}");
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
