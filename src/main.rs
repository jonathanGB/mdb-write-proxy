use mysql::prelude::*;

type Message = (u64, u64, String);

fn main() {
    let mysql_url =  "mysql://root:@localhost:3306/mdb";
    let pool = mysql::Pool::new(mysql_url).unwrap();
    let mut conn = pool.get_conn().unwrap();


    let res : Option<Message> = conn.query_first(r"select sender_id, sendee_id, content from messages;").unwrap();

    println!("Hello, world! {:?}", res);

    let user_dms_write_policy = r"
    INSERT into Messages (sender_id, sendee_id, content)
    SELECT :sender_id, :sendee_id, :content
    WHERE EXISTS (
        SELECT 1
        FROM Users
        WHERE id = :sendee_id AND is_open_dms = True
    );";
    let message_to_insert = (1, 2, "message from #1 to #2", 2);

    use mysql::TxOpts;
    use mysql::IsolationLevel;
    use mysql::Error;
    use mysql::Result;

    let opts = TxOpts::default();
    let opts = opts.set_isolation_level(Some(IsolationLevel::Serializable));
    
    let mut tx = conn.start_transaction(opts).unwrap();
    let res : Result<()> = tx.exec_drop(user_dms_write_policy, message_to_insert);
    let num_rows : Result<Option<u64>> = tx.query_first("select row_count();");
    let success = tx.rollback();
    let num = conn.affected_rows();
    println!("insert {:?}\n{:?} vs {:?} ... {:?}", res, num, num_rows, success.is_ok());
}
