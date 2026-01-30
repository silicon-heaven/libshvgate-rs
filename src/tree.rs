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
    pub fn from_yaml(input: &str) -> Self {
        // TODO
        Self { nodes_description: Default::default() }
    }

    pub fn from_typeinfo(input: &str) -> Self {
        // TODO
        Self { nodes_description: Default::default() }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum SampleType {
    #[default]
    Continuos,
    Discrete,
}

#[derive(Debug, Clone)]
pub struct NodeDescription {
    pub methods: Vec<MetaMethod>,
    pub sample_type: SampleType,
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
