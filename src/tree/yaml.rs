use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(tag = "version")]
pub enum TreeDocument {
    #[serde(rename = "1")]
    V1(v1::TreeDefinition),
}

mod v1 {
    use std::collections::BTreeMap;

    use serde::{Deserialize, Deserializer};

    fn deserialize_tree<'de, D>(
        deserializer: D,
    ) -> Result<BTreeMap<String, String>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_yaml_ng::Value::deserialize(deserializer)?;
        let mut result = BTreeMap::new();

        fn flatten(
            value: &serde_yaml_ng::Value,
            prefix: Option<String>,
            out: &mut BTreeMap<String, String>,
        ) -> Result<(), String> {
            match value {
                serde_yaml_ng::Value::Mapping(map) => {
                    for (k, v) in map {
                        let key = k
                            .as_str()
                            .ok_or_else(|| format!("Non-string key `{k:?}` in tree"))?;

                        let full_key = prefix
                            .as_ref()
                            .map_or_else(|| key.to_string(), |p| shvrpc::join_path!(p, key));

                        flatten(v, Some(full_key), out)?;
                    }
                }

                serde_yaml_ng::Value::String(s) => {
                    let key = prefix.ok_or_else(|| format!("Leaf string without key context: `{s}`"))?;
                    out.insert(key, s.clone());
                }

                _ => return Err(format!("Tree values must be either mappings or strings. Cannot process value: `{value:?}`"))
            }

            Ok(())
        }

        flatten(&value, None, &mut result)
            .map_err(serde::de::Error::custom)?;

        Ok(result)
    }

    #[derive(Debug, Deserialize)]
    pub struct TreeDefinition {
        #[serde(deserialize_with = "deserialize_tree")]
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

#[cfg(test)]
    mod tests {
        use super::*;
        use std::collections::BTreeMap;

        fn expected_tree() -> BTreeMap<String, String> {
            BTreeMap::from([
                (
                    "voltage/V1".to_string(),
                    "Voltage".to_string(),
                ),
                (
                    "voltage/V1/value".to_string(),
                    "VoltageValue".to_string(),
                ),
                (
                    "voltage/V1/settings".to_string(),
                    "VoltageSettings".to_string(),
                ),
            ])
        }

        #[test]
        fn flat_yaml_is_parsed_correctly() {
            let yaml = r#"
tree:
  voltage/V1: Voltage
  voltage/V1/value: VoltageValue
  voltage/V1/settings: VoltageSettings

nodes: {}
"#;

            let parsed: TreeDefinition = serde_yaml_ng::from_str(yaml).unwrap();
            assert_eq!(parsed.tree, expected_tree());
        }

        #[test]
        fn one_level_nested_yaml_is_equivalent() {
            let yaml = r#"
tree:
  voltage:
    V1: Voltage
    V1/value: VoltageValue
    V1/settings: VoltageSettings

nodes: {}
"#;

            let parsed: TreeDefinition = serde_yaml_ng::from_str(yaml).unwrap();
            assert_eq!(parsed.tree, expected_tree());
        }

        #[test]
        fn mixed_flat_and_nested_yaml_is_equivalent() {
            let yaml = r#"
tree:
  voltage:
    V1: Voltage
  voltage/V1/value: VoltageValue
  voltage/V1/settings: VoltageSettings

nodes: {}
"#;

            let parsed: TreeDefinition = serde_yaml_ng::from_str(yaml).unwrap();
            assert_eq!(parsed.tree, expected_tree());
        }

        #[test]
        fn fails_on_non_string_key() {
            let yaml = r#"
tree:
  123:
    V1: Voltage
nodes: {}
"#;

            let result: Result<TreeDefinition, _> = serde_yaml_ng::from_str(yaml);

            assert!(result.is_err());

            let err = result.unwrap_err().to_string();
            assert!(
                err.contains("Non-string key"),
                "Unexpected error: {err}"
            );
        }

        #[test]
        fn fails_on_invalid_leaf_value_number() {
            let yaml = r#"
tree:
  voltage:
    V1: 42
nodes: {}
"#;

            let result: Result<TreeDefinition, _> = serde_yaml_ng::from_str(yaml);

            assert!(result.is_err());

            let err = result.unwrap_err().to_string();
            assert!(
                err.contains("Tree values must be either mappings or strings"),
                "Unexpected error: {err}"
            );
        }

        #[test]
        fn fails_on_invalid_leaf_value_bool() {
            let yaml = r#"
tree:
  voltage:
    V1: true
nodes: {}
"#;

            let result: Result<TreeDefinition, _> = serde_yaml_ng::from_str(yaml);

            assert!(result.is_err());
        }

        #[test]
        fn fails_when_nested_value_is_sequence() {
            let yaml = r#"
tree:
  voltage:
    - V1
    - V2
nodes: {}
"#;

            let result: Result<TreeDefinition, _> = serde_yaml_ng::from_str(yaml);

            assert!(result.is_err());

            let err = result.unwrap_err().to_string();
            assert!(
                err.contains("Tree values must be either mappings or strings"),
                "Unexpected error: {err}"
            );
        }

        #[test]
        fn fails_on_tree_root_string_leaf() {
            let yaml = r#"
tree: Voltage
nodes: {}
"#;

            let result: Result<TreeDefinition, _> = serde_yaml_ng::from_str(yaml);
            assert!(result.is_err());

            let err = result.unwrap_err().to_string();
            assert!(
                err.contains("Leaf string without key context"),
                "Unexpected error: {err}"
            );
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
