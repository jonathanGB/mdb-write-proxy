use mysql_async::error::Error as mysql_error;
use mysql_async::prelude::*;
use mysql_async::{IsolationLevel, Pool, TransactionOptions};
use noria::consensus::ZookeeperAuthority;
use noria::{ControllerHandle, DataType};
use sqlparser::ast::Statement::CreateTable;
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::{Parser, ParserError};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::time::Instant;

#[derive(Clone)]
pub struct WriteProxy {
    pool: Pool,
    noria_db: ControllerHandle<ZookeeperAuthority>,
    tables: HashMap<String, TableInfo>,
    write_handles: HashMap<String, noria::Table>,
}

#[derive(Debug)]
pub enum WriteProxyErr<'a> {
    MysqlErr(mysql_error),
    MissingTable(&'a str),
    ParserErr(ParserError),
    MysqlToNoriaDatatypeErr(&'static str),
    NoriaErr(failure::Error),
    TableErr(noria::error::TableError),
    PolicyNotInserted,
}

#[derive(Clone, Debug)]
pub struct TableInfo {
    policies: Option<PoliciesInfo>,
    column_names: Option<Vec<String>>,
}

#[derive(Clone, Debug)]
struct PoliciesInfo {
    startup_policies: Vec<String>,
    predicate: String,
}

#[derive(Default, Debug)]
pub struct Timing {
    constructed_at: Option<Instant>,
    returned_at: Option<Instant>,
    start_insert: Option<Instant>,
    start_transaction: Option<Instant>,
    completed_policies: Vec<Instant>,
    completed_insert: Option<Instant>,
    completed_select_count: Option<Instant>,
    completed_commit: Option<Instant>,
}

impl Timing {
    pub fn new() -> Self {
        let mut t = Timing::default();
        t.constructed_at = Some(Instant::now());
        t
    }

    pub fn set_returned(&mut self) {
        self.returned_at = Some(Instant::now());
    }

    pub fn total_time_millis(&self) -> Option<u128> {
        match (self.constructed_at, self.returned_at) {
            (None, _) | (_, None) => None,
            (Some(start), Some(returned)) => Some(returned.duration_since(start).as_millis()),
        }
    }
}

impl WriteProxy {
    pub fn default_pool() -> Result<Pool, mysql_error> {
        Pool::from_url("mysql://root:@localhost:3306/mdb")
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
    ) -> Result<Self, mysql_error> {
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
                let conn = match self.pool.get_conn().await {
                    Ok(conn) => conn,
                    Err(e) => return Err(WriteProxyErr::MysqlErr(e)),
                };
                if let Err(e) = conn.drop_query(stmt).await {
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
        record: Vec<mysql_async::Value>,
        mut timing: Timing,
    ) -> (Timing, Result<(), WriteProxyErr<'a>>) {
        timing.start_insert = Some(Instant::now());

        let table = self.tables.get(table_name);
        if table.is_none() || table.unwrap().column_names.is_none() {
            timing.set_returned();
            return (timing, Err(WriteProxyErr::MissingTable(table_name)));
        }

        let table_info = table.unwrap();

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
            // 1. Start transaction, with serializable isolation level.
            let mut tx_options = TransactionOptions::default();
            tx_options.set_isolation_level(Some(IsolationLevel::Serializable));
            let mut tx = match self.pool.start_transaction(tx_options).await {
                Ok(tx) => tx,
                Err(e) => {
                    timing.set_returned();
                    return (timing, Err(WriteProxyErr::MysqlErr(e)));
                }
            };
            timing.start_transaction = Some(Instant::now());

            let column_names = table_info.column_names.as_ref().unwrap();

            let params: Vec<(String, mysql_async::Value)> = column_names
                .iter()
                .enumerate()
                .map(|(i, column_name)| (column_name.clone(), record[i].clone().into()))
                .collect();

            // 2. Execute startup policies.
            for policy in startup_policies {
                tx = match tx.drop_exec(policy, &params).await {
                    Ok(tx) => tx,
                    Err(e) => {
                        timing.set_returned();
                        return (timing, Err(WriteProxyErr::MysqlErr(e)));
                    }
                };
                timing.completed_policies.push(Instant::now());
            }

            /*             tx = match tx
                .first::<_, (u8, u8)>("SELECT @user_with_open_dms, @user_who_follow_sender;")
                .await
            {
                Ok((tx, Some((first, second)))) => {
                    println!("got {:?}, {:?}", first, second);
                    tx
                }
                _ => unreachable!(),
            }; */

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
            tx = match tx.drop_exec(insert_predicate, params).await {
                Ok(tx) => tx,
                Err(e) => {
                    timing.set_returned();
                    return (timing, Err(WriteProxyErr::MysqlErr(e)));
                }
            };
            timing.completed_insert = Some(Instant::now());

            // 4. Test the insertion. If `row_count()` is 0, then we know that
            // no rows were inserted. If it's more than 0, then insertion passed
            // all the policies.
            tx = match tx.first::<_, u8>("SELECT row_count();").await {
                Ok((_, Some(num))) if num == 0 => {
                    timing.set_returned();
                    return (timing, Err(WriteProxyErr::PolicyNotInserted));
                }
                Ok((tx, Some(_))) => tx, // Insert worked, move on.
                Ok(_) => unreachable!("SELECT row_count() must return something!"),
                Err(e) => {
                    timing.set_returned();
                    return (timing, Err(WriteProxyErr::MysqlErr(e)));
                }
            };
            timing.completed_select_count = Some(Instant::now());

            // Don't propagate write to Noria, unless the transaction commits.
            if let Err(e) = tx.commit().await {
                timing.set_returned();
                return (timing, Err(WriteProxyErr::MysqlErr(e)));
            }
            timing.completed_commit = Some(Instant::now());
        }

        // 4a. Propagate write to Noria!
        let mut record_noria = Vec::new();
        for value in record {
            match DataType::try_from(value) {
                Ok(record) => record_noria.push(record),
                Err(e) => {
                    timing.set_returned();
                    return (timing, Err(WriteProxyErr::MysqlToNoriaDatatypeErr(e)));
                }
            };
        }

        let res = match self.write_handles.get_mut(table_name) {
            Some(write_handle) => write_handle
                .insert(record_noria)
                .await
                .map_err(|e| WriteProxyErr::TableErr(e)),
            None => unreachable!(format!("No table handle registered for {}", table_name)),
        };
        timing.set_returned();
        (timing, res)
    }
}
