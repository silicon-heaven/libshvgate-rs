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
        let tree_def: yaml::TreeDefinition = serde_yaml_ng::from_value(yaml_value)
            .unwrap_or_else(|err| panic!("Bad tree definition: {err}"));

        let mut nodes_description = BTreeMap::new();

        for (path, node_type_name) in tree_def.tree {
            let methods = tree_def
                .nodes
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

pub(crate) fn method_has_signal(mm: &MetaMethod, signal: impl AsRef<str>) -> bool {
    let signal = signal.as_ref();
    match &mm.signals {
        SignalsDefinition::Static(def) => def.iter().any(|(name, _)| signal == *name),
        SignalsDefinition::Dynamic(def) => def.contains_key(signal),
    }
}

impl ShvTree {
    pub(crate) fn from_definition(definition: ShvTreeDefinition) -> Self {
        let cache = definition
            .nodes_description
            .iter()
            .flat_map(|(path, descr)| descr
                .methods
                .iter()
                .filter(|mm| method_has_signal(mm, SIG_CHNG))
                .map(|mm| PathMethod(path.clone(), String::from(mm.name.clone())))
            )
            .map(|path_method| (path_method, RwLock::new(CachedValue(None))))
            .collect();

        Self { cache, definition }
    }

    pub(crate) fn update_value(&self, path: impl AsRef<str>, method: impl AsRef<str>, new_value: &RpcValue) -> Option<bool> {
        let path = path.as_ref();
        let method = method.as_ref();
        let locked_value = self.cache.get(&PathMethod(String::from(path), String::from(method)))?;
        let mut value = locked_value.write().unwrap();
        Some(value.update(new_value))
    }

    pub(crate) fn read_value(&self, path: impl Into<String>, method: impl Into<String>) -> Option<RpcValue> {
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
                .find(|mm| mm.name == method && method_has_signal(mm, signal))
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use shvrpc::metamethod::{MetaMethod, Flags};

    fn getter_method(name: &str, signals: SignalsDefinition) -> MetaMethod {
        MetaMethod::new(
            name.to_owned(),
            Flags::IsGetter,
            shvrpc::metamethod::AccessLevel::Read,
            "",
            "",
            signals,
            ""
        )
    }

    fn non_getter_method(name: &str) -> MetaMethod {
        MetaMethod::new(
            name.to_owned(),
            Flags::empty(),
            shvrpc::metamethod::AccessLevel::Read,
            "",
            "",
            SignalsDefinition::Static(&[]),
            ""
        )
    }

    fn tree_with_methods(path: &str, methods: Vec<MetaMethod>) -> ShvTree {
        let mut nodes_description = BTreeMap::new();
        nodes_description.insert(
            path.into(),
            NodeDescription { methods },
        );

        ShvTree::from_definition(ShvTreeDefinition { nodes_description })
    }

    #[test]
    fn from_definition_caches_only_getters() {
        let tree = tree_with_methods(
            "dev/node",
            vec![
                getter_method("getA", SignalsDefinition::Static(&[])),
                non_getter_method("setA"),
            ],
        );

        // Getter exists
        assert!(tree.read_value("dev/node", "getA").is_none());

        // Non-getter should not be cached at all
        assert!(tree.update_value("dev/node", "setA", &RpcValue::from(1)).is_none());
        assert!(tree.read_value("dev/node", "setA").is_none());
    }

    #[test]
    fn update_and_read_roundtrip() {
        let tree = tree_with_methods(
            "dev/node",
            vec![getter_method("getA", SignalsDefinition::Static(&[]))],
        );

        let val = RpcValue::from(42);

        // Initially no cached value
        assert_eq!(tree.read_value("dev/node", "getA"), None);

        // Update should hit cache
        let res = tree.update_value("dev/node", "getA", &val);
        assert!(res.is_some());

        // Now value should be readable
        assert_eq!(tree.read_value("dev/node", "getA"), Some(val));
    }

    #[test]
    fn update_unknown_path_or_method_returns_none() {
        let tree = tree_with_methods(
            "dev/node",
            vec![getter_method("getA", SignalsDefinition::Static(&[]))],
        );

        assert_eq!(
            tree.update_value("dev/other", "getA", &RpcValue::from(1)),
            None
        );
        assert_eq!(
            tree.update_value("dev/node", "missing", &RpcValue::from(1)),
            None
        );
    }

    #[test]
    fn method_has_signal_static_and_dynamic() {
        let static_signals = SignalsDefinition::Static(&[
            ("sig1", None),
            ("sig2", None),
        ]);

        let mut dyn_map = std::collections::BTreeMap::new();
        dyn_map.insert("dynSig".into(), Default::default());
        let dynamic_signals = SignalsDefinition::Dynamic(dyn_map);

        let tree = tree_with_methods(
            "dev/node",
            vec![
                getter_method("m1", static_signals),
                getter_method("m2", dynamic_signals),
            ],
        );

        assert!(tree.method_has_signal("dev/node", "m1", "sig1"));
        assert!(!tree.method_has_signal("dev/node", "m1", "nope"));

        assert!(tree.method_has_signal("dev/node", "m2", "dynSig"));
        assert!(!tree.method_has_signal("dev/node", "m2", "sig1"));
    }

    #[test]
    fn snapshot_keys_only_methods_with_sig_chng() {
        let with_chng = getter_method(
            "m1",
            SignalsDefinition::Static(&[(SIG_CHNG, None)]),
        );
        let without_chng =
            getter_method("m2", SignalsDefinition::Static(&[]));

        let tree = tree_with_methods("dev/node", vec![with_chng, without_chng]);

        let keys: Vec<_> = tree.snapshot_keys().collect();

        assert_eq!(keys.len(), 1);
        let PathMethod(path, method) = keys[0];
        assert_eq!(path, "dev/node");
        assert_eq!(method, "m1");
    }
}
