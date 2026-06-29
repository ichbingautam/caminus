use crate::source::ChangeEvent;
use wasmtime::*;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum TransformError {
    #[error("Wasm engine error: {0}")]
    Wasm(#[from] wasmtime::Error),
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("Missing exported function: {0}")]
    MissingExport(String),
    #[error("Wasm memory access error: {0}")]
    MemoryAccess(#[from] wasmtime::MemoryAccessError),
}

pub trait Transformer: Send + Sync {
    fn transform(&self, event: ChangeEvent) -> Result<ChangeEvent, TransformError>;
}

pub struct WasmTransformer {
    engine: Engine,
    module: Module,
}

impl WasmTransformer {
    pub fn new(wat_or_wasm_bytes: &[u8]) -> Result<Self, TransformError> {
        let engine = Engine::default();
        let module = Module::new(&engine, wat_or_wasm_bytes)?;
        Ok(Self { engine, module })
    }
}

impl Transformer for WasmTransformer {
    fn transform(&self, event: ChangeEvent) -> Result<ChangeEvent, TransformError> {
        let mut store = Store::new(&self.engine, ());
        let instance = Instance::new(&mut store, &self.module, &[])?;

        // Get exports from WASM module
        let alloc = instance
            .get_typed_func::<i32, i32>(&mut store, "alloc")
            .map_err(|_| TransformError::MissingExport("alloc".to_string()))?;
            
        let transform_fn = instance
            .get_typed_func::<(i32, i32), i32>(&mut store, "transform")
            .map_err(|_| TransformError::MissingExport("transform".to_string()))?;
            
        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| TransformError::Wasm(wasmtime::Error::msg("Missing memory export")))?;

        // Serialize Event to JSON
        let json_bytes = serde_json::to_vec(&event)?;
        let len = json_bytes.len() as i32;

        // Allocate memory block in guest WASM memory
        let guest_ptr = alloc.call(&mut store, len)?;

        // Copy JSON payload into guest memory
        memory.write(&mut store, guest_ptr as usize, &json_bytes)?;

        // Invoke the transform function
        let out_len = transform_fn.call(&mut store, (guest_ptr, len))?;

        // Read the resulting payload from guest memory
        let mut out_buffer = vec![0u8; out_len as usize];
        memory.read(&store, guest_ptr as usize, &mut out_buffer)?;

        // Deserialize the transformed JSON payload back into ChangeEvent
        let transformed_event: ChangeEvent = serde_json::from_slice(&out_buffer)?;
        Ok(transformed_event)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::Operation;
    use chrono::Utc;

    #[test]
    fn test_wasm_passthrough() {
        // Compile simple passthrough WAT
        let wat = r#"
            (module
              (memory (export "memory") 1)
              (func (export "alloc") (param i32) (result i32)
                i32.const 0
              )
              (func (export "transform") (param i32 i32) (result i32)
                local.get 1
              )
            )
        "#;

        let transformer = WasmTransformer::new(wat.as_bytes()).unwrap();

        let event = ChangeEvent {
            id: "evt-123".into(),
            source_database: "caminus_db".into(),
            source_table_or_collection: "users".into(),
            operation: Operation::Create,
            timestamp: Utc::now(),
            key: serde_json::json!({ "id": 1 }),
            before: None,
            after: Some(serde_json::json!({ "id": 1, "name": "John" })),
            transaction_id: None,
            offset: "1".into(),
        };

        let transformed = transformer.transform(event.clone()).unwrap();
        assert_eq!(transformed.id, event.id);
        assert_eq!(transformed.source_table_or_collection, event.source_table_or_collection);
    }

    #[test]
    fn test_wasm_inline_replace() {
        // Compile WAT that searches for "users" and replaces with "audit"
        let wat = r#"
            (module
              (memory (export "memory") 1)
              (func (export "alloc") (param i32) (result i32)
                i32.const 0
              )
              (func (export "transform") (param $ptr i32) (param $len i32) (result i32)
                (local $i i32)
                (local $end i32)
                local.get $ptr
                local.set $i
                local.get $ptr
                local.get $len
                i32.add
                i32.const 4
                i32.sub
                local.set $end
                (block
                  (loop
                    local.get $i
                    local.get $end
                    i32.ge_s
                    br_if 1
                    
                    local.get $i
                    i32.load8_u
                    i32.const 117 ;; 'u'
                    i32.eq
                    if
                      local.get $i
                      i32.const 1
                      i32.add
                      i32.load8_u
                      i32.const 115 ;; 's'
                      i32.eq
                      if
                        local.get $i
                        i32.const 2
                        i32.add
                        i32.load8_u
                        i32.const 101 ;; 'e'
                        i32.eq
                        if
                          local.get $i
                          i32.const 3
                          i32.add
                          i32.load8_u
                          i32.const 114 ;; 'r'
                          i32.eq
                          if
                            local.get $i
                            i32.const 4
                            i32.add
                            i32.load8_u
                            i32.const 115 ;; 's'
                            i32.eq
                            if
                              local.get $i
                              i32.const 97 ;; 'a'
                              i32.store8
                              local.get $i
                              i32.const 1
                              i32.add
                              i32.const 117 ;; 'u'
                              i32.store8
                              local.get $i
                              i32.const 2
                              i32.add
                              i32.const 100 ;; 'd'
                              i32.store8
                              local.get $i
                              i32.const 3
                              i32.add
                              i32.const 105 ;; 'i'
                              i32.store8
                              local.get $i
                              i32.const 4
                              i32.add
                              i32.const 116 ;; 't'
                              i32.store8
                            end
                          end
                        end
                      end
                    end
                    
                    local.get $i
                    i32.const 1
                    i32.add
                    local.set $i
                    br 0
                  )
                )
                local.get $len
              )
            )
        "#;

        let transformer = WasmTransformer::new(wat.as_bytes()).unwrap();

        let event = ChangeEvent {
            id: "evt-123".into(),
            source_database: "caminus_db".into(),
            source_table_or_collection: "users".into(),
            operation: Operation::Create,
            timestamp: Utc::now(),
            key: serde_json::json!({ "id": 1 }),
            before: None,
            after: Some(serde_json::json!({ "id": 1, "name": "John" })),
            transaction_id: None,
            offset: "1".into(),
        };

        let transformed = transformer.transform(event).unwrap();
        // The table name "users" should be replaced with "audit"
        assert_eq!(transformed.source_table_or_collection, "audit");
    }
}
