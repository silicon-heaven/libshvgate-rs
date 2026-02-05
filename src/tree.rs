use std::collections::BTreeMap;
use std::sync::RwLock;

use shvclient::clientnode::{METH_GET, MetaMethod};
use shvclient::shvproto::RpcValue;

#[derive(Debug)]
pub(crate) struct ShvTree {
    pub(crate) values: BTreeMap<String, RwLock<RpcValue>>,
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
        let values = definition
            .nodes_description
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
            self.definition.nodes_description
                .get(path)
                .is_some_and(|descr| descr.sample_type == SampleType::Discrete)
        {
            *value = new_value.clone();
            return Some(true);
        }
        Some(false)
    }

    pub(crate) fn read(&self, path: impl AsRef<str>) -> Option<RpcValue> {
        self.values
            .get(path.as_ref())
            .map(RwLock::read)
            .map(Result::unwrap)
            .as_deref()
            .map(Clone::clone)
    }
}
