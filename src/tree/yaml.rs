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
