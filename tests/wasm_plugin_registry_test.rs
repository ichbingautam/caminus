use chrono::Utc;
use serde_json::json;

use caminus::source::{ChangeEvent, Operation};
use caminus::transform::registry::WasmPluginRegistry;

const PASSTHROUGH_WAT_V1: &str = r#"
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

const INLINE_REPLACE_WAT_V2: &str = r#"
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

#[test]
fn test_live_wasm_plugin_registration_and_hot_reloading() {
    let registry = WasmPluginRegistry::new();

    // 1. Register Plugin v1.0.0 (Passthrough SMT)
    registry
        .register_plugin("audit_rewriter", "1.0.0", PASSTHROUGH_WAT_V1.as_bytes())
        .expect("Failed to register plugin v1.0.0");

    let meta1 = registry.get_metadata("audit_rewriter").unwrap();
    assert_eq!(meta1.version, "1.0.0");

    let event = ChangeEvent {
        id: "evt-wasm-10".to_string(),
        source_database: "db".to_string(),
        source_table_or_collection: "users".to_string(),
        operation: Operation::Create,
        timestamp: Utc::now(),
        key: json!({ "id": 10 }),
        before: None,
        after: Some(json!({ "id": 10, "name": "Alice" })),
        transaction_id: Some("tx-10".to_string()),
        offset: "offset-10".to_string(),
    };

    // Execute with v1.0.0
    let res1 = registry
        .execute_transform("audit_rewriter", &event)
        .unwrap()
        .unwrap();
    assert_eq!(res1.source_table_or_collection, "users");

    // 2. Hot-Reload Plugin to v2.0.0 (Inline String Replacement SMT)
    registry
        .hot_reload_plugin("audit_rewriter", "2.0.0", INLINE_REPLACE_WAT_V2.as_bytes())
        .expect("Failed to hot-reload plugin v2.0.0");

    let meta2 = registry.get_metadata("audit_rewriter").unwrap();
    assert_eq!(meta2.version, "2.0.0");

    // Execute with v2.0.0 (table 'users' dynamically mutated to 'audit')
    let res2 = registry
        .execute_transform("audit_rewriter", &event)
        .unwrap()
        .unwrap();
    assert_eq!(res2.source_table_or_collection, "audit");
}
