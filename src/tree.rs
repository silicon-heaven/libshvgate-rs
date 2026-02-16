use std::collections::BTreeMap;
use std::sync::RwLock;

use shvclient::clientnode::{METH_GET, MetaMethod, SIG_CHNG};
use shvclient::shvproto::RpcValue;
use shvrpc::metamethod::{Flags, SignalsDefinition};
use shvrpc::typeinfo::{FieldDescriptionMethods, TypeInfo};

mod yaml;

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub(crate) struct PathMethod(pub(crate) String, pub(crate) String);

pub(crate) trait PathMethodKey {
    fn path(&self) -> &str;
    fn method(&self) -> &str;

    fn as_key(&self) -> &dyn PathMethodKey
    where
        Self: Sized
    {
            self
    }
}

impl PathMethodKey for PathMethod {
    fn path(&self) -> &str {
        &self.0
    }

    fn method(&self) -> &str {
        &self.1
    }
}

impl<A, B> PathMethodKey for (A, B)
where
    A: AsRef<str>,
    B: AsRef<str>,
{
    fn path(&self) -> &str {
        self.0.as_ref()
    }

    fn method(&self) -> &str {
        self.1.as_ref()
    }
}

impl PartialEq for dyn PathMethodKey + '_ {
    fn eq(&self, other: &Self) -> bool {
        self.path() == other.path() && self.method() == other.method()
    }
}

impl Eq for dyn PathMethodKey + '_ { }

impl PartialOrd for dyn PathMethodKey + '_ {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for dyn PathMethodKey + '_ {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.path().cmp(other.path())
            .then_with(|| self.method().cmp(other.method()))
    }
}

impl<'a> std::borrow::Borrow<dyn PathMethodKey + 'a> for PathMethod {
    fn borrow(&self) -> &(dyn PathMethodKey + 'a) {
        self
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CachedValue(pub(crate) Option<RpcValue>);

impl CachedValue {
    fn update(&mut self, new_value: &RpcValue) -> bool {
        match &mut self.0 {
            Some(value) if new_value != value => {
                *value = new_value.clone();
                true
            }
            Some(_) => false,
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
    pub fn from_yaml(input: impl AsRef<str>) -> Result<Self, String> {
        let mut yaml_value: serde_yaml_ng::Value = serde_yaml_ng::from_str(input.as_ref())
            .map_err(|err| format!("Bad YAML definition: {err}"))?;
        yaml_value.apply_merge().map_err(|err| format!("Cannot merge YAML anchors: {err}"))?;
        let tree_document: yaml::TreeDocument = serde_yaml_ng::from_value(yaml_value)
            .map_err(|err| format!("Bad tree definition: {err}"))?;

        let mut nodes_description = BTreeMap::new();

        match tree_document {
            yaml::TreeDocument::V1(tree_definition) => {
                for (path, node_type_name) in tree_definition.tree {
                    let methods = tree_definition
                        .nodes
                        .get(&node_type_name)
                        .into_iter()
                        .flatten()
                        .map(|m| MetaMethod::from(m.clone()))
                        .collect();

                    nodes_description.insert(path, NodeDescription { methods });
                }
            }
        }

        Ok(Self { nodes_description })
    }

    pub fn from_typeinfo(type_info: &TypeInfo) -> Self {
        let device_nodes: BTreeMap<String, BTreeMap<String, Vec<MetaMethod>>> = type_info
            .device_descriptions()
            .iter()
            .map(|(device_type, device_descr)| {
                let prop_methods: BTreeMap<String, Vec<MetaMethod>> = device_descr
                    .properties()
                    .iter()
                    .map(|prop| {
                        let methods = from_methods_description(prop.methods());
                        (prop.name().into(), methods)
                    })
                .collect();
                (device_type.clone(), prop_methods)
            })
        .collect();

        fn from_methods_description(methods: impl Into<Vec<shvrpc::typeinfo::MethodDescription>>) -> Vec<MetaMethod> {
            let methods = methods.into();
            let has_chng = methods.iter().any(|m| m.name == SIG_CHNG);
            methods.into_iter()
                .filter_map(|m| {
                    let flags = Flags::from_bits_retain(m.flags);
                    if flags.contains(Flags::IsSignal) {
                        return None
                    }
                    let mut signals: BTreeMap<_,_> = m.signals.into_iter().collect();
                    if m.name == METH_GET && has_chng {
                        signals.entry(SIG_CHNG.into()).or_insert(None);
                    }
                    Some(MetaMethod::new(
                        m.name,
                        flags,
                        m.access,
                        m.param,
                        m.result,
                        SignalsDefinition::Dynamic(signals),
                        m.description,
                ))
                })
                .collect()
        }

        let mut nodes_description: BTreeMap<String, NodeDescription> = type_info
            .device_paths()
            .iter()
            .filter_map(|(device_path, device_type)| {
                if device_path.is_empty() {
                    // Do not allow to override the root node as it is special
                    return None
                }
                Some(device_nodes
                    .get(device_type)?
                    .iter()
                    .map(|(prop_name, methods)| {
                        let node_path = shvrpc::join_path!(device_path, prop_name);
                        (node_path, NodeDescription { methods: methods.to_owned() })
                    })
                )}
            )
            .flatten()
            .collect();

        for (path, prop) in type_info.property_deviations() {
            if path.is_empty() {
                continue
            }
            let methods = from_methods_description(prop.methods());
            nodes_description.insert(path.into(), NodeDescription { methods });
        }

        Self { nodes_description }
    }
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

    pub(crate) fn update_value(&self, path: impl AsRef<str>, method: impl AsRef<str>, new_value: &RpcValue) -> Result<bool, String> {
        let path = path.as_ref();
        let method = method.as_ref();
        let locked_value = self.cache.get((path, method).as_key())
            .ok_or_else(|| format!("Cache for method `{path}:{method}` does not exist"))?;
        let mut value = locked_value.write().expect("Value cache mutex should not be poisoned");
        Ok(value.update(new_value))
    }

    pub(crate) fn read_value(&self, path: impl AsRef<str>, method: impl AsRef<str>) -> Option<RpcValue> {
        self.cache
            .get((path, method).as_key())
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

    fn cached_method(name: &str) -> MetaMethod {
        MetaMethod::new(
            name.to_owned(),
            Flags::None,
            shvrpc::metamethod::AccessLevel::Read,
            "",
            "",
            SignalsDefinition::Static(&[("chng", None)]),
            ""
        )
    }

    fn non_cached_method(name: &str) -> MetaMethod {
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
    fn caches_only_chng_methods() {
        let tree = tree_with_methods(
            "dev/node",
            vec![
                cached_method("getA"),
                non_cached_method("setA"),
            ],
        );

        // Getter exists
        assert!(tree.update_value("dev/node", "getA", &RpcValue::from(1)).is_ok());
        assert_eq!(tree.read_value("dev/node", "getA"), Some(RpcValue::from(1)));

        // Non-getter should not be cached at all
        assert!(tree.update_value("dev/node", "setA", &RpcValue::from(1)).is_err());
        assert!(tree.read_value("dev/node", "setA").is_none());
    }

    #[test]
    fn update_and_read_roundtrip() {
        let tree = tree_with_methods(
            "dev/node",
            vec![cached_method("getA")],
        );

        let val = RpcValue::from(42);

        // Initially no cached value
        assert_eq!(tree.read_value("dev/node", "getA"), None);

        // Update should hit cache
        let res = tree.update_value("dev/node", "getA", &val);
        assert_eq!(res, Ok(true));

        // Now value should be readable
        assert_eq!(tree.read_value("dev/node", "getA"), Some(val));
    }

    #[test]
    fn update_unknown_path_or_method_returns_none() {
        let tree = tree_with_methods(
            "dev/node",
            vec![cached_method("getA")],
        );

        assert!(tree.update_value("dev/other", "getA", &RpcValue::from(1)).is_err());
        assert!(tree.update_value("dev/node", "missing", &RpcValue::from(1)).is_err());
    }

    #[test]
    fn method_has_signal_static_and_dynamic() {
        let static_signals = SignalsDefinition::Static(&[
            ("sig1", None),
            ("sig2", None),
        ]);

        let mut dyn_map = std::collections::BTreeMap::new();
        dyn_map.insert("dynSig".into(), None);
        let dynamic_signals = SignalsDefinition::Dynamic(dyn_map);

        let tree = tree_with_methods(
            "dev/node",
            vec![
            MetaMethod::new(
                "m1",
                Flags::None,
                shvrpc::metamethod::AccessLevel::Read,
                "",
                "",
                static_signals,
                ""
            ),
            MetaMethod::new(
                "m2",
                Flags::None,
                shvrpc::metamethod::AccessLevel::Read,
                "",
                "",
                dynamic_signals,
                ""
            ),
            ],
        );

        assert!(tree.method_has_signal("dev/node", "m1", "sig1"));
        assert!(!tree.method_has_signal("dev/node", "m1", "nope"));

        assert!(tree.method_has_signal("dev/node", "m2", "dynSig"));
        assert!(!tree.method_has_signal("dev/node", "m2", "sig1"));
    }

    #[test]
    fn snapshot_keys_only_methods_with_sig_chng() {
        let with_chng = cached_method("m1");
        let without_chng = non_cached_method("m2");

        let tree = tree_with_methods("dev/node", vec![with_chng, without_chng]);

        let keys: Vec<_> = tree.snapshot_keys().collect();

        assert_eq!(keys.len(), 1);
        let PathMethod(path, method) = keys[0];
        assert_eq!(path, "dev/node");
        assert_eq!(method, "m1");
    }
}
