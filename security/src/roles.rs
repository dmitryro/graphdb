// security/src/roles.rs
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use anyhow::Result; // Use anyhow for error handling as per your Cargo.toml

#[derive(Debug, Deserialize, Clone)] // Added Clone for easier use with Arc/Rc
pub struct RoleConfig {
    pub id: u32,
    pub permissions: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)] // Added Clone
pub struct RolesConfig {
    pub roles: HashMap<String, RoleConfig>,
    #[serde(skip)]
    role_id_map: HashMap<u32, RoleConfig>, // New map for ID-based lookup
}

impl RolesConfig {
    pub fn from_yaml_file(path: &str) -> Result<Self> {
        let content = fs::read_to_string(path)?;
        let mut config: RolesConfig = serde_yaml::from_str(&content)?;

        // Populate the role_id_map for quick lookups by ID
        config.role_id_map = config.roles.values()
                                    .map(|role_cfg| (role_cfg.id, role_cfg.clone()))
                                    .collect();
        Ok(config)
    }

    // Changed to return Option<&RoleConfig> for more flexibility
    pub fn get_role_config_by_id(&self, role_id: u32) -> Option<&RoleConfig> {
        self.role_id_map.get(&role_id)
    }

    pub fn has_permission(&self, role_id: u32, permission_name: &str) -> bool {
        self.get_role_config_by_id(role_id)
            .map_or(false, |role_cfg| {
                role_cfg.permissions.contains(&permission_name.to_string()) ||
                role_cfg.permissions.contains(&"superuser".to_string()) // Check for superuser permission
            })
    }
}
