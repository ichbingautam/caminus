use crate::source::ChangeEvent;
use crate::transform::{TransformError, Transformer, WasmTransformEngine};
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::RwLock;

#[derive(Debug, Clone)]
pub struct PluginMetadata {
    pub name: String,
    pub version: String,
    pub enabled: bool,
    pub loaded_at: DateTime<Utc>,
}

pub struct WasmPluginRegistry {
    plugins: RwLock<HashMap<String, (PluginMetadata, WasmTransformEngine)>>,
}

impl Default for WasmPluginRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl WasmPluginRegistry {
    pub fn new() -> Self {
        Self {
            plugins: RwLock::new(HashMap::new()),
        }
    }

    /// Registers a new WASM plugin module with specified name and version.
    pub fn register_plugin(
        &self,
        name: impl Into<String>,
        version: impl Into<String>,
        wasm_bytes: &[u8],
    ) -> Result<(), TransformError> {
        let plugin_name = name.into();
        let plugin_ver = version.into();
        let engine = WasmTransformEngine::new(wasm_bytes)?;

        let metadata = PluginMetadata {
            name: plugin_name.clone(),
            version: plugin_ver.clone(),
            enabled: true,
            loaded_at: Utc::now(),
        };

        let mut lock = self.plugins.write().unwrap();
        lock.insert(plugin_name.clone(), (metadata, engine));

        println!(
            "[WASM REGISTRY] Successfully registered plugin '{}' v{}",
            plugin_name, plugin_ver
        );

        Ok(())
    }

    /// Atomically hot-reloads an active WASM plugin version without dropping in-flight streams.
    pub fn hot_reload_plugin(
        &self,
        name: &str,
        new_version: impl Into<String>,
        wasm_bytes: &[u8],
    ) -> Result<(), TransformError> {
        let new_ver = new_version.into();
        let engine = WasmTransformEngine::new(wasm_bytes)?;

        let metadata = PluginMetadata {
            name: name.to_string(),
            version: new_ver.clone(),
            enabled: true,
            loaded_at: Utc::now(),
        };

        let mut lock = self.plugins.write().unwrap();
        lock.insert(name.to_string(), (metadata, engine));

        println!(
            "[WASM REGISTRY] Atomically hot-reloaded plugin '{}' to v{}",
            name, new_ver
        );

        Ok(())
    }

    /// Executes a registered active WASM plugin transform on an incoming ChangeEvent.
    pub fn execute_transform(
        &self,
        plugin_name: &str,
        event: &ChangeEvent,
    ) -> Result<Option<ChangeEvent>, TransformError> {
        let lock = self.plugins.read().unwrap();
        let (metadata, engine) = lock.get(plugin_name).ok_or_else(|| {
            TransformError::MissingExport(format!("Plugin '{}' not registered", plugin_name))
        })?;

        if !metadata.enabled {
            return Ok(Some(event.clone()));
        }

        engine.transform(event.clone()).map(Some)
    }

    /// Retrieves metadata for a registered plugin.
    pub fn get_metadata(&self, plugin_name: &str) -> Option<PluginMetadata> {
        let lock = self.plugins.read().unwrap();
        lock.get(plugin_name).map(|(meta, _)| meta.clone())
    }

    /// Returns the total count of registered active plugins.
    pub fn len(&self) -> usize {
        let lock = self.plugins.read().unwrap();
        lock.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DUMMY_WASM_VAT: &[u8] = b"(module)";

    #[test]
    fn test_wasm_plugin_registration_and_metadata() {
        let registry = WasmPluginRegistry::new();
        assert!(registry.is_empty());

        let res = registry.register_plugin("anonymizer", "1.0.0", DUMMY_WASM_VAT);
        assert!(res.is_ok());
        assert_eq!(registry.len(), 1);

        let meta = registry.get_metadata("anonymizer").unwrap();
        assert_eq!(meta.name, "anonymizer");
        assert_eq!(meta.version, "1.0.0");
        assert!(meta.enabled);
    }
}
