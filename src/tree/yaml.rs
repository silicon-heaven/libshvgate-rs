use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(tag = "version")]
pub enum TreeDocument {
    #[serde(rename = "1")]
    V1(v1::TreeDefinition),
}

mod v1 {
    use std::collections::BTreeMap;

    use serde::Deserialize;

    #[derive(Debug, Deserialize)]
    pub struct TreeDefinition {
        pub tree: BTreeMap<String, String>,
        pub nodes: BTreeMap<String, Vec<MethodDefinition>>,
    }

    #[derive(Debug, Deserialize, Clone)]
    pub struct MethodDefinition {
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

    impl From<MethodDefinition> for shvrpc::metamethod::MetaMethod {
        fn from(value: MethodDefinition) -> Self {
            let flags = FromIterator::from_iter(value.flags.into_iter().map(Into::into));
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
}

#[cfg(test)]
mod tests {
    use crate::ShvTreeDefinition;

    use shvrpc::metamethod::{Flags, MetaMethod, AccessLevel, SignalsDefinition};

    #[test]
    fn from_yaml_empty_tree() {
        let yaml = r#"
version: "1"
tree: {}
nodes: {}
"#;

        let tree_def = ShvTreeDefinition::from_yaml(yaml).unwrap();
        assert!(tree_def.nodes_description.is_empty());
    }

    #[test]
    fn from_yaml_with_nodes_no_methods() {
        let yaml = r#"
version: "1"
tree:
  foo/bar/node1: NodeType1
  foo/bar/node2: NodeType2

nodes:
  NodeType1: []
  NodeType2: []
"#;

        let tree_def = ShvTreeDefinition::from_yaml(yaml).unwrap();

        assert_eq!(tree_def.nodes_description.len(), 2);
        assert!(tree_def.nodes_description.get("foo/bar/node1").unwrap().methods.is_empty());
        assert!(tree_def.nodes_description.get("foo/bar/node2").unwrap().methods.is_empty());
    }

    #[test]
    fn from_yaml_with_nodes_and_methods() {
        let yaml = r#"
version: "1"
tree:
  path/node1: NodeType1

nodes:
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

        let tree_def = ShvTreeDefinition::from_yaml(yaml).unwrap();
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
            SignalsDefinition::Static(_) => panic!("Expected dynamic signals"),
        }
        assert_eq!(method.description, "Test method");
    }

    #[test]
    fn from_yaml_invalid_yaml() {
        let yaml = r#"
version: "1"
nodes
  node1: NodeType1
"#;

        assert!(ShvTreeDefinition::from_yaml(yaml).is_err());
    }
}
