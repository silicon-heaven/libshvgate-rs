use serde::Deserialize;
use std::collections::BTreeMap;


#[derive(Debug, Deserialize)]
pub struct Tree {
    pub version: String,
    pub nodes: BTreeMap<String, String>,

    #[serde(default)]
    pub node_methods: BTreeMap<String, Vec<Method>>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Method {
    pub name: String,

    #[serde(default)]
    pub flags: Vec<MethodFlag>,

    #[serde(default)]
    pub access: Option<String>,

    #[serde(default)]
    pub param: Option<String>,

    #[serde(default)]
    pub result: Option<String>,

    #[serde(default)]
    pub signals: BTreeMap<String, Option<String>>,

    #[serde(default)]
    pub description: Option<String>,

    // #[serde(default)]
    // pub retval: Option<serde_yaml_ng::Value>,
}

#[derive(Debug, Deserialize, Clone, Copy)]
pub enum MethodFlag {
    IsGetter,
    IsSetter,
    LargeResultHint,
    UserIDRequired,
}

impl From<MethodFlag> for shvrpc::metamethod::Flags {
    fn from(value: MethodFlag) -> Self {
        match value {
            MethodFlag::IsGetter => Self::IsGetter,
            MethodFlag::IsSetter => Self::IsSetter,
            MethodFlag::LargeResultHint => Self::LargeResultHint,
            MethodFlag::UserIDRequired => Self::UserIDRequired,
        }
    }
}

impl From<Method> for shvrpc::metamethod::MetaMethod {
    fn from(value: Method) -> Self {
        let flags = shvrpc::metamethod::Flags::from_iter(value.flags.into_iter().map(Into::into));
        let access = value.access
            .and_then(|access_str| shvrpc::metamethod::AccessLevel::from_str(&access_str))
            .unwrap_or_default();
        shvrpc::metamethod::MetaMethod::new(
            value.name,
            flags,
            access,
            value.param.unwrap_or_default(),
            value.result.unwrap_or_default(),
            shvrpc::metamethod::SignalsDefinition::Dynamic(value.signals),
            value.description.unwrap_or_default()
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::ShvTreeDefinition;

    use shvrpc::metamethod::{Flags, MetaMethod, AccessLevel, SignalsDefinition};

    #[test]
    fn from_yaml_empty_tree() {
        let yaml = r#"
version: "1.0"
nodes: {}
"#;

        let tree_def = ShvTreeDefinition::from_yaml(yaml);
        assert!(tree_def.nodes_description.is_empty());
    }

    #[test]
    fn from_yaml_with_nodes_no_methods() {
        let yaml = r#"
version: "1.0"
nodes:
  foo/bar/node1: NodeType1
  foo/bar/node2: NodeType2
"#;

        let tree_def = ShvTreeDefinition::from_yaml(yaml);

        assert_eq!(tree_def.nodes_description.len(), 2);
        assert!(tree_def.nodes_description.get("foo/bar/node1").unwrap().methods.is_empty());
        assert!(tree_def.nodes_description.get("foo/bar/node2").unwrap().methods.is_empty());
    }

    #[test]
    fn from_yaml_with_nodes_and_methods() {
        let yaml = r#"
version: "1.0"
nodes:
  path/node1: NodeType1

node_methods:
  NodeType1:
    - name: "method1"
      flags: [IsGetter, UserIDRequired]
      access: "rd"
      param: "int"
      result: "string"
      description: "Test method"
      signals:
        signal1: "int"
"#;

        let tree_def = ShvTreeDefinition::from_yaml(yaml);
        let node = tree_def.nodes_description.get("path/node1").unwrap();
        assert_eq!(node.methods.len(), 1);

        let method: &MetaMethod = &node.methods[0];
        assert_eq!(method.name, "method1");
        assert!(method.flags.contains(Flags::IsGetter));
        assert!(method.flags.contains(Flags::UserIDRequired));
        assert_eq!(method.access, AccessLevel::Read);
        assert_eq!(method.param, "int");
        assert_eq!(method.result, "string");
        match &method.signals {
            SignalsDefinition::Dynamic(signals) => {
                assert_eq!(signals.get("signal1").unwrap().as_deref(), Some("int"));
            }
            _ => panic!("Expected dynamic signals"),
        }
        assert_eq!(method.description, "Test method");
    }

    #[test]
    #[should_panic(expected = "Bad YAML definition")]
    fn from_yaml_invalid_yaml() {
        let yaml = r#"
version: "1.0"
nodes
  node1: NodeType1
"#;

        ShvTreeDefinition::from_yaml(yaml);
    }
}
