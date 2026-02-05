use std::collections::BTreeMap;
use std::sync::RwLock;

use shvclient::clientnode::{MetaMethod, SIG_CHNG};
use shvclient::shvproto::RpcValue;
use shvrpc::metamethod::SignalsDefinition;

mod yaml;

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub(crate) struct PathMethod(pub(crate) String, pub(crate) String);

#[derive(Debug, Clone)]
pub(crate) struct CachedValue(pub(crate) Option<RpcValue>);

impl CachedValue {
    fn update(&mut self, new_value: &RpcValue) -> bool {
        match &mut self.0 {
            Some(value) => if new_value != value {
                *value = new_value.clone();
                true
            } else {
                false
            },
            None => {
                self.0 = Some(new_value.clone());
                true
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct ShvTree {
    pub(crate) cache: BTreeMap<PathMethod, RwLock<CachedValue>>,
    pub(crate) definition: ShvTreeDefinition,
}

#[derive(Debug, Clone)]
pub struct ShvTreeDefinition {
    pub nodes_description: BTreeMap<String, NodeDescription>,
}

impl ShvTreeDefinition {
    pub fn from_yaml(input: impl AsRef<str>) -> Self {
        let mut yaml_value: serde_yaml_ng::Value = serde_yaml_ng::from_str(input.as_ref())
            .unwrap_or_else(|err| panic!("Bad YAML definition: {err}"));
        yaml_value.apply_merge().unwrap_or_else(|err| panic!("Cannot merge YAML anchors: {err}"));
        let tree: yaml::Tree = serde_yaml_ng::from_value(yaml_value)
            .unwrap_or_else(|err| panic!("Bad tree definition: {err}"));

        let mut nodes_description = BTreeMap::new();

        for (path, node_type_name) in tree.nodes {
            let methods = tree
                .node_methods
                .get(&node_type_name)
                .into_iter()
                .flatten()
                .map(|m| MetaMethod::from(m.clone()))
                .collect();

            nodes_description.insert(path, NodeDescription { methods });
        }

        Self { nodes_description }
    }

    // pub fn from_typeinfo(input: &str) -> Self {
    //     // TODO
    //     Self { nodes_description: Default::default() }
    // }
}

#[derive(Debug, Clone)]
pub struct NodeDescription {
    pub methods: Vec<MetaMethod>,
}

impl ShvTree {
    pub(crate) fn from_definition(definition: ShvTreeDefinition) -> Self {
        let cache = definition
            .nodes_description
            .iter()
            .flat_map(|(path, descr)| descr
                .methods
                .iter()
                .filter_map(|mm| mm.flags.contains(shvrpc::metamethod::Flags::IsGetter).then(|| PathMethod(path.clone(), String::from(mm.name.clone()))))
            )
            .map(|path_method| (path_method, RwLock::new(CachedValue(None))))
            .collect();

        Self { cache, definition }
    }

    pub(crate) fn update(&self, path: impl AsRef<str>, method: impl AsRef<str>, new_value: &RpcValue) -> Option<bool> {
        let path = path.as_ref();
        let method = method.as_ref();
        let Some(locked_value) = self.cache.get(&PathMethod(String::from(path), String::from(method))) else {
            return None;
        };
        let mut value = locked_value.write().unwrap();
        Some(value.update(new_value))
    }

    pub(crate) fn read(&self, path: impl Into<String>, method: impl Into<String>) -> Option<RpcValue> {
        self.cache
            .get(&PathMethod(path.into(), method.into()))
            .map(RwLock::read)
            .map(Result::unwrap)
            .as_deref()
            .and_then(|cached_value| cached_value.0.clone())
    }

    pub(crate) fn method_has_signal(&self, path: impl AsRef<str>, method: impl AsRef<str>, signal: impl AsRef<str>) -> bool {
        let signal = signal.as_ref();
        let method = method.as_ref();
        self.definition.nodes_description
            .get(path.as_ref())
            .and_then(|descr| descr
                .methods
                .iter()
                .find(|mm| mm.name == method &&
                    match &mm.signals {
                        SignalsDefinition::Static(def) => def.iter().find(|(name, _)| signal == *name).is_some(),
                        SignalsDefinition::Dynamic(def) => def.contains_key(signal),
                    })
            )
            .is_some()
    }

    pub(crate) fn snapshot_keys(&self) -> impl Iterator<Item = &PathMethod> {
        self
            .cache
            .keys()
            .filter(|PathMethod(path, method)| self.method_has_signal(path, method, SIG_CHNG))
    }
}
