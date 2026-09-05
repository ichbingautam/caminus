use chrono::Utc;
use serde_json::json;

use caminus::router::TenantRouter;
use caminus::source::{ChangeEvent, Operation};

#[tokio::test]
async fn test_tenant_quota_isolation_and_routing() {
    let tenant_router = TenantRouter::new(8, 2, 10.0);

    // Configure tenant-A with capacity 1
    tenant_router.set_tenant_quota("tenant-A", 1, 1.0);

    let event_a = ChangeEvent {
        id: "evt-t-a-1".to_string(),
        source_database: "db_prod".to_string(),
        source_table_or_collection: "orders".to_string(),
        operation: Operation::Create,
        timestamp: Utc::now(),
        key: json!({ "id": 1 }),
        before: None,
        after: Some(json!({ "id": 1, "tenant_id": "tenant-A" })),
        transaction_id: Some("tx-a-1".to_string()),
        offset: "off-a-1".to_string(),
    };

    let event_b = ChangeEvent {
        id: "evt-t-b-1".to_string(),
        source_database: "db_prod".to_string(),
        source_table_or_collection: "orders".to_string(),
        operation: Operation::Create,
        timestamp: Utc::now(),
        key: json!({ "id": 2 }),
        before: None,
        after: Some(json!({ "id": 2, "tenant_id": "tenant-B" })),
        transaction_id: Some("tx-b-1".to_string()),
        offset: "off-b-1".to_string(),
    };

    // 1. First event for tenant-A succeeds
    let part_a1 = tenant_router.route_event(&event_a).await;
    assert!(part_a1.is_some());

    // 2. Second event for tenant-A is rejected (quota exceeded)
    let part_a2 = tenant_router.route_event(&event_a).await;
    assert!(part_a2.is_none());

    // 3. tenant-B is isolated and still succeeds (default capacity is 2)
    let part_b1 = tenant_router.route_event(&event_b).await;
    assert!(part_b1.is_some());

    let part_b2 = tenant_router.route_event(&event_b).await;
    assert!(part_b2.is_some());

    let part_b3 = tenant_router.route_event(&event_b).await;
    assert!(part_b3.is_none());
}
