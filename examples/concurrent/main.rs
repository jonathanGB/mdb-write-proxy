use mdb_write_proxy::{Timing, WriteProxy, WriteProxyErr};
use noria::ControllerHandle;
use std::fs;
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::thread::sleep;
use std::time::{Duration, Instant};

#[tokio::main]
async fn main() {
    let db = ControllerHandle::from_zk("localhost:2181/c26")
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

    if let Err(e) = write_proxy.extend_recipe(schema_sql).await {
        return println!("Problem extending recipe: {:?}", e);
    }

    let SLEEP_MICROS = 10;
    let NUM_RESULTS = 10000;

    let mut results = Vec::with_capacity(NUM_RESULTS);
    let (tx, rx): (Sender<Results>, Receiver<Results>) = mpsc::channel();
    for i in 0..NUM_RESULTS {
        let mut proxy = write_proxy.clone();
        let tx_thread = tx.clone();
        let timing = Timing::new();

        tokio::spawn(async move {
            let record = if i % 5 == 0 {
                vec![
                    32401.into(),
                    1001.into(),
                    "message from #32401 to #1001".into(),
                ]
            } else {
                vec![2.into(), 1.into(), "message from #2 to #1".into()]
            };

            let (timing, res) = proxy.insert("Messages", record, timing).await;
            tx_thread
                .send((i, timing.total_time_millis(), res.is_ok()))
                .unwrap();
        });

        sleep(Duration::from_micros(SLEEP_MICROS));
    }

    for _ in 0..NUM_RESULTS {
        results.push(rx.recv().unwrap());
    }

    for (i, total_time, is_ok) in results {
        println!("{} {:?} {:?}", i, total_time, is_ok);
    }
}

type Results = (usize, Option<u128>, bool);