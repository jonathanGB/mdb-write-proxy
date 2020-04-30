use mdb_write_proxy::{WriteProxy, WriteProxyErr};
use noria::ControllerHandle;
use std::fs;

#[tokio::main]
async fn main() {
    let db = ControllerHandle::from_zk("localhost:2181/go65")
        .await
        .unwrap();
    let policy_content = fs::read_to_string("simple/twitter-write-policies.json").unwrap();
    let policy_content_json: serde_json::Value = serde_json::from_str(&policy_content).unwrap();
    let policies = match WriteProxy::parse_policy_content(policy_content_json) {
        Ok(policies) => policies,
        Err(e) => return println!("Problem parsing policy file: {}", e),
    };

    let pool = WriteProxy::default_pool().unwrap();
    let mut write_proxy = WriteProxy::new(pool, db, policies);

    let schema_sql = r"
    CREATE TABLE Messages(
        sender_id int,
        sendee_id int,
        content VARCHAR(100)
    );
    ";

    let queries = r"
    QUERY MessagesViewAll:
        SELECT * FROM Messages WHERE sender_id = ?;
    ";

    if let Err(e) = write_proxy.extend_recipe(schema_sql).await {
        return println!("Problem extending recipe: {:?}", e);
    }

    match write_proxy.extend_recipe(queries).await {
        // The SQL parser fails if a query has a `?`, but as long as
        // the recipe was sent to Noria, it is ok.
        Ok(_) | Err(WriteProxyErr::ParserErr(_)) => {}
        Err(e) => {
            return println!("Problem extending queries recipe: {:?}", e);
        }
    }

    let mut messages_view = match write_proxy.view("MessagesViewAll").await {
        Ok(messages_view) => messages_view,
        Err(e) => return println!("Error getting view `MessagesAll`: {:?}", e),
    };

    match write_proxy
        .insert(
            "Messages",
            vec![3.into(), 4.into(), "message from #3 to #4".into()],
        )
        .await
    {
        Ok(_) => println!("Success inserting!"),
        Err(e) => println!("Problem inserting: {:?}", e),
    };

    println!(
        "{:?}",
        messages_view.lookup(&[3.into()], true).await.unwrap()
    );

    match write_proxy
        .insert(
            "Messages",
            vec![3.into(), 2.into(), "message from #3 to #2".into()],
        )
        .await
    {
        Ok(_) => println!("Success inserting!"),
        Err(e) => println!("Problem inserting: {:?}", e),
    };

    println!(
        "{:?}",
        messages_view.lookup(&[3.into()], true).await.unwrap()
    );

    match write_proxy
        .insert(
            "Messages",
            vec![3.into(), 4.into(), "message from #3 to #4".into()],
        )
        .await
    {
        Ok(_) => println!("Success inserting!"),
        Err(e) => println!("Problem inserting: {:?}", e),
    };

    match write_proxy
        .insert(
            "Messages",
            vec![3.into(), 1.into(), "message from #3 to #1".into()],
        )
        .await
    {
        Ok(_) => println!("Success inserting!"),
        Err(e) => println!("Problem inserting: {:?}", e),
    };

    println!(
        "{:?}",
        messages_view.lookup(&[3.into()], true).await.unwrap()
    );
}
