use serde::Deserialize;
use std::collections::HashMap;
use std::fs;

#[derive(Debug, Deserialize)]
pub struct RoleConfig {
    pub id: u32,
    pub permissions: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct RolesConfig {
    pub roles: HashMap<String, RoleConfig>,
}

impl RolesConfig {
    pub fn from_yaml_file(path: &str) -> anyhow::Result<Self> {
        let content = fs::read_to_string(path)?;
        let config: RolesConfig = serde_yaml::from_str(&content)?;
        Ok(config)
    }

    pub fn get_permissions(&self, role_id: u32) -> Option<&[String]> {
        self.roles.values()
            .find(|r| r.id == role_id)
            .map(|r| r.permissions.as_slice())
    }
}
