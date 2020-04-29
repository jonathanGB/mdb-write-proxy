use mysql::prelude::*;
use mysql::{IsolationLevel, Pool, TxOpts};
use noria::consensus::ZookeeperAuthority;
use noria::{ControllerHandle, DataType};
use sqlparser::ast::Statement::CreateTable;
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::{Parser, ParserError};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fs;
use std::time::Instant;

#[tokio::main]
async fn main() {
    let db = ControllerHandle::from_zk("localhost:2181/gogo8")
        .await
        .unwrap();
    let policy_content = fs::read_to_string("src/twitter-write-policies.json").unwrap();
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
            vec![3.into(), 2.into(), "message from #3 to #2".into()],
        )
        .await
    {
        Ok(delay) => println!("Success inserting!: {:?}", delay),
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
        Ok(delay) => println!("Success inserting!: {:?}", delay),
        Err(e) => println!("Problem inserting: {:?}", e),
    };

    println!(
        "{:?}",
        messages_view.lookup(&[3.into()], true).await.unwrap()
    );

    match write_proxy
        .insert(
            "Messages",
            vec![3.into(), 1.into(), "message from #3 to #1".into()],
        )
        .await
    {
        Ok(delay) => println!("Success inserting!: {:?}", delay),
        Err(e) => println!("Problem inserting: {:?}", e),
    };

    println!(
        "{:?}",
        messages_view.lookup(&[3.into()], true).await.unwrap()
    );

    match write_proxy
        .insert(
            "Messages",
            vec![
                1083.into(),
                1000737.into(),
                "message from #1083 to #1000737".into(),
            ],
        )
        .await
    {
        Ok(delay) => println!("Success inserting!: {:?}", delay),
        Err(e) => println!("Problem inserting: {:?}", e),
    };

    println!(
        "{:?}",
        messages_view.lookup(&[1083.into()], true).await.unwrap()
    );
}

struct WriteProxy {
    pool: Pool,
    noria_db: ControllerHandle<ZookeeperAuthority>,
    tables: HashMap<String, TableInfo>,
    write_handles: HashMap<String, noria::Table>,
}

#[derive(Debug)]
enum WriteProxyErr<'a> {
    MysqlErr(mysql::Error),
    MissingTable(&'a str),
    ParserErr(ParserError),
    MysqlToNoriaDatatypeErr(&'static str),
    NoriaErr(failure::Error),
    TableErr(noria::error::TableError),
    PolicyNotInserted,
}

#[derive(Debug)]
struct TableInfo {
    policies: Option<PoliciesInfo>,
    column_names: Option<Vec<String>>,
}

#[derive(Debug)]
struct PoliciesInfo {
    startup_policies: Vec<String>,
    predicate: String,
}

#[derive(Debug, Default, Clone)]
struct InsertDelay {
    fetch_table_info: Option<u128>,
    start_transaction: Option<u128>,
    populate_params: Option<u128>,
    run_policies: Option<Vec<u128>>,
    run_insert: Option<u128>,
    run_select_count: Option<u128>,
    commit: Option<u128>,
    convert_to_noria_type: Option<u128>,
    send_to_noria_server: Option<u128>,
}

impl WriteProxy {
    pub fn default_pool() -> Result<mysql::Pool, mysql::Error> {
        Pool::new("mysql://root:@localhost:3306/mdb")
    }

    pub async fn view(&mut self, view: &str) -> Result<noria::View, failure::Error> {
        self.noria_db.view(view).await
    }

    pub fn parse_policy_content(
        policies: serde_json::Value,
    ) -> Result<HashMap<String, TableInfo>, String> {
        let mut tables = HashMap::default();

        let policies = match policies.as_object() {
            Some(policies) => policies,
            None => return Err("Policy file should be a JSON object!".to_string()),
        };

        for (table, table_obj) in policies {
            let table_policies = match table_obj.as_object() {
                Some(table_policies) => table_policies,
                None => return Err(format!("Entry {} should be an object!", table)),
            };

            let startup = match table_policies.get("startup") {
                Some(startup) => startup,
                None => return Err(format!("Entry {} has no `startup` set!", table)),
            };
            let startup = match startup.as_array() {
                Some(startup) => startup,
                None => {
                    return Err(format!(
                        "Entry {} has a `startup` set, but it should be an array!",
                        table
                    ))
                }
            };

            let mut startup_policies = Vec::new();
            for startup_policy in startup {
                match startup_policy.as_str() {
                    Some(startup_policy) => startup_policies.push(startup_policy.to_string()),
                    None => {
                        return Err(format!(
                            "Entry {} `startup` policies should be strings!",
                            table
                        ))
                    }
                };
            }

            let predicate = match table_policies.get("predicate") {
                Some(predicate) => predicate,
                None => return Err(format!("Entry {} has no `predicate` set!", table)),
            };
            let predicate = match predicate.as_str() {
                Some(predicate) => predicate.to_string(),
                None => {
                    return Err(format!(
                        "Entry {} has a `predicate` set, but it should be a string!",
                        table
                    ))
                }
            };

            tables.insert(
                table.clone(),
                TableInfo {
                    policies: Some(PoliciesInfo {
                        startup_policies,
                        predicate,
                    }),
                    column_names: None,
                },
            );
        }

        Ok(tables)
    }

    pub fn new(
        pool: Pool,
        noria_db: ControllerHandle<ZookeeperAuthority>,
        tables: HashMap<String, TableInfo>,
    ) -> Self {
        Self {
            pool,
            noria_db,
            tables,
            write_handles: HashMap::default(),
        }
    }

    pub fn new_with_default_pool(
        noria_db: ControllerHandle<ZookeeperAuthority>,
        tables: HashMap<String, TableInfo>,
    ) -> Result<Self, mysql::Error> {
        match Self::default_pool() {
            Ok(pool) => Ok(Self::new(pool, noria_db, tables)),
            Err(e) => Err(e),
        }
    }

    pub async fn extend_recipe(&mut self, stmt: &str) -> Result<(), WriteProxyErr<'_>> {
        // Propagate to Noria!
        if let Err(e) = self.noria_db.extend_recipe(stmt).await {
            return Err(WriteProxyErr::NoriaErr(e));
        }

        let queries = match Parser::parse_sql(&MySqlDialect {}, stmt.to_string()) {
            Ok(queries) => queries,
            Err(e) => return Err(WriteProxyErr::ParserErr(e)),
        };

        // If we have a `Create Table` statement:
        //   1. Propagate it to our MySQL database.
        //   2. Keep track of the columns of the new table
        //      (for subsequently interpolating inserted values).
        //   3. Get a handle to the Noria table, and store it.
        for query in queries {
            if let CreateTable { name, columns, .. } = query {
                // 1. Create the table in MySQL.
                let mut conn = match self.pool.get_conn() {
                    Ok(conn) => conn,
                    Err(e) => return Err(WriteProxyErr::MysqlErr(e)),
                };
                if let Err(e) = conn.query_drop(stmt) {
                    return Err(WriteProxyErr::MysqlErr(e));
                }
                // TODO: we only remember the first part of the table name.
                // If we were given `my_db.my_table`, this would not work properly.
                // This should not be a problem, because Noria doesn't have such notation.
                let table_name = name.0[0].clone();
                let column_names = columns
                    .iter()
                    .map(|column| column.name.clone())
                    .collect::<Vec<String>>();

                // 2. Remember the columns of the table.
                self.tables
                    .entry(table_name.clone())
                    .or_insert(TableInfo {
                        policies: None,
                        column_names: None,
                    })
                    .column_names = Some(column_names);

                // 3. Get a write handle.
                match self.noria_db.table(&table_name).await {
                    Ok(table) => self.write_handles.insert(table_name, table),
                    Err(e) => return Err(WriteProxyErr::NoriaErr(e)),
                };
            }
        }

        Ok(())
    }

    pub async fn insert<'a>(
        &mut self,
        table_name: &'a str,
        records: Vec<mysql::Value>,
    ) -> Result<InsertDelay, (InsertDelay, WriteProxyErr<'a>)> {
        let mut delays = InsertDelay::default();

        let now = Instant::now();
        let table = self.tables.get(table_name);
        if table.is_none() || table.unwrap().column_names.is_none() {
            return Err((delays, WriteProxyErr::MissingTable(table_name)));
        }

        let table_info = table.unwrap();
        delays.fetch_table_info = Some(now.elapsed().as_micros());

        // If we have policies for this table, we must go through:
        //   1. Get a transaction conn to the SQL table.
        //   2. Execute the startup policies.
        //   3. Execute the conditional insertion (predicate).
        //   4. Validate that the insertion passed:
        //     4a. If so, we propagate the records to Noria.
        if let Some(PoliciesInfo {
            ref startup_policies,
            ref predicate,
        }) = table_info.policies
        {
            let now = Instant::now();
            // 1. Start transaction, with serializable isolation level.
            let tx_options =
                TxOpts::default().set_isolation_level(Some(IsolationLevel::Serializable));
            let mut tx = match self.pool.start_transaction(tx_options) {
                Ok(tx) => tx,
                Err(e) => return Err((delays, WriteProxyErr::MysqlErr(e))),
            };
            delays.start_transaction = Some(now.elapsed().as_micros());

            let now = Instant::now();
            let column_names = table_info.column_names.as_ref().unwrap();

            let params: Vec<(String, mysql::Value)> = column_names
                .iter()
                .enumerate()
                .map(|(i, column_name)| (column_name.clone(), records[i].clone().into()))
                .collect();
            delays.populate_params = Some(now.elapsed().as_micros());
            delays.run_policies = Some(vec![]);

            // 2. Execute startup policies.
            for policy in startup_policies {
                let now = Instant::now();
                if let Err(e) = tx.exec_drop(policy, &params) {
                    return Err((delays, WriteProxyErr::MysqlErr(e)));
                }
                delays
                    .run_policies
                    .as_mut()
                    .unwrap()
                    .push(now.elapsed().as_micros());
            }

            let now = Instant::now();
            // Interpolate the insertion predicate.
            let insert_predicate = format!(
                "INSERT INTO {} ({}) SELECT {} {}",
                table_name,
                column_names.join(","),
                column_names
                    .iter()
                    .map(|col| format!(":{}", col))
                    .collect::<Vec<String>>()
                    .join(","),
                predicate
            );

            // 3. Execute the predicate.
            if let Err(e) = tx.exec_drop(insert_predicate, params) {
                return Err((delays, WriteProxyErr::MysqlErr(e)));
            }
            delays.run_insert = Some(now.elapsed().as_micros());
            let now = Instant::now();

            // 4. Test the insertion. If `row_count()` is 0, then we know that
            // no rows were inserted. If it's more than 0, then insertion passed
            // all the policies.
            match tx.query_first::<u8, _>("SELECT row_count();") {
                Ok(Some(num)) if num == 0 => {
                    return Err((delays, WriteProxyErr::PolicyNotInserted))
                }
                Ok(Some(_)) => {} // Insert worked, move on.
                Ok(None) => unreachable!("SELECT row_count() must return something!"),
                Err(e) => return Err((delays, WriteProxyErr::MysqlErr(e))),
            };

            delays.run_select_count = Some(now.elapsed().as_micros());
            let now = Instant::now();

            // Don't propagate write to Noria, unless the transaction commits.
            if let Err(e) = tx.commit() {
                return Err((delays, WriteProxyErr::MysqlErr(e)));
            }
            delays.commit = Some(now.elapsed().as_micros());
        }

        let now = Instant::now();
        // 4a. Propagate write to Noria!
        let mut records_noria = Vec::new();
        for record in records {
            match DataType::try_from(record) {
                Ok(record) => records_noria.push(record),
                Err(e) => return Err((delays, WriteProxyErr::MysqlToNoriaDatatypeErr(e))),
            };
        }
        delays.convert_to_noria_type = Some(now.elapsed().as_micros());
        let now = Instant::now();

        match self.write_handles.get_mut(table_name) {
            Some(write_handle) => write_handle
                .insert(records_noria)
                .await
                .map_err(|e| (delays.clone(), WriteProxyErr::TableErr(e)))
                .map(|_| {
                    delays.send_to_noria_server = Some(now.elapsed().as_micros());
                    delays
                }),
            None => unreachable!(format!("No table handle registered for {}", table_name)),
        }
    }
}
