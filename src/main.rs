use mysql::{IsolationLevel, Pool, TxOpts};
use mysql::prelude::*;
use sqlparser::ast::Statement::CreateTable;
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::{Parser, ParserError};
use std::collections::HashMap;
use std::fs;

fn main() {
    let policy_content = fs::read_to_string("src/twitter-write-policies.json").unwrap();
    let policy_content_json : serde_json::Value = serde_json::from_str(&policy_content).unwrap();
    let policies = match WriteProxy::parse_policy_content(policy_content_json) {
        Ok(policies) => policies,
        Err(e) => return println!("Problem parsing policy file: {}", e),
    };

    let pool = WriteProxy::default_pool().unwrap();
    let mut write_proxy = WriteProxy::new(pool, policies);

    let messages_sql = r"
    CREATE TABLE Messages(
        sender_id int,
        sendee_id int,
        content VARCHAR(100)
    );
    ";

    if let Err(e) = write_proxy.extend_recipe(messages_sql) {
        println!("Problem creating Messages table: {:?}", e);
        return;
    }

    match write_proxy.insert("Messages", vec![3.into(), 2.into(), "message from #3 to #2".into()]) {
        Ok(_) => println!("Success inserting!"),
        Err(e) => println!("Problem inserting: {:?}", e), 
    };
    match write_proxy.insert("Messages", vec![2.into(), 3.into(), "message from #3 to #3".into()]) {
        Ok(_) => println!("Success inserting!"),
        Err(e) => println!("Problem inserting: {:?}", e), 
    };
}

struct WriteProxy {
    pool: Pool,
    tables: HashMap<String, TableInfo>,
}

#[derive(Debug)]
enum WriteProxyErr<'a> {
    MysqlErr(mysql::Error),
    MissingTable(&'a str),
    ParserErr(ParserError),
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

impl WriteProxy {
    pub fn default_pool() -> Result<mysql::Pool, mysql::Error> {
        Pool::new("mysql://root:@localhost:3306/mdb")
    }

    pub fn parse_policy_content(policies: serde_json::Value) -> Result<HashMap<String, TableInfo>, String> {
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
                None => return Err(format!("Entry {} has a `startup` set, but it should be an array!", table)),
            };

            let mut startup_policies = Vec::new();
            for startup_policy in startup {
                match startup_policy.as_str() {
                    Some(startup_policy) => startup_policies.push(startup_policy.to_string()),
                    None => return Err(format!("Entry {} `startup` policies should be strings!", table)),
                };
            }

            let predicate = match table_policies.get("predicate") {
                Some(predicate) => predicate,
                None => return Err(format!("Entry {} has no `predicate` set!", table)),
            };
            let predicate = match predicate.as_str() {
                Some(predicate) => predicate.to_string(),
                None => return Err(format!("Entry {} has a `predicate` set, but it should be a string!", table)),
            };

            tables.insert(table.clone(), TableInfo{
                policies: Some(PoliciesInfo{
                    startup_policies,
                    predicate,
                }),
                column_names: None,
            });
        }

        Ok(tables)
    }

    pub fn new(pool: Pool, tables: HashMap<String, TableInfo>) -> Self {
        Self {
            pool,
            tables,
        }
    }

    pub fn new_with_default_pool(tables: HashMap<String, TableInfo>) -> Result<Self, mysql::Error> {
        match Self::default_pool() {
            Ok(pool) => Ok(Self::new(pool, tables)),
            Err(e) => Err(e),
        }
    }

    pub fn extend_recipe(&mut self, stmt: &str) -> Result<(), WriteProxyErr> {
        let query = match Parser::parse_sql(&MySqlDialect{}, stmt.to_string()) {
            Ok(mut query) => query.swap_remove(0),
            Err(e) => return Err(WriteProxyErr::ParserErr(e)),
        };

        // If we have a `Create Table` statement, propagate it to our SQL table.
        // Also, keep track of the columns, so that subsequent INSERT/UPDATEs values
        // can be interpolated.
        if let CreateTable{name, columns, ..} = query {
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
            let column_names = columns.iter().map(|column| column.name.clone()).collect::<Vec<String>>();

            self.tables.entry(table_name)
                .or_insert(TableInfo{
                    policies: None,
                    column_names: None,
                })
                .column_names = Some(column_names);
        }

        // TODO: Propagate to Noria!
        Ok(())
    }

    pub fn insert<'a>(&'a mut self, table_name: &'a str, records: Vec<mysql::Value>) -> Result<(), WriteProxyErr<'a>> {
        let table = self.tables.get(table_name);
        if table.is_none() || table.unwrap().column_names.is_none() {
            return Err(WriteProxyErr::MissingTable(table_name));
        }

        let table_info = table.unwrap();

        // If we have policies for this table, we must go through:
        //   1. Get a conn to the SQL table.
        //   2. Interpolate the startup policies + predicate
        //   3. If the insertion passed, we can continue to Noria.
        if let Some(PoliciesInfo {ref startup_policies, ref predicate}) = table_info.policies {
            let tx_options = TxOpts::default().set_isolation_level(Some(IsolationLevel::Serializable));
            let mut tx = match self.pool.start_transaction(tx_options) {
                Ok(tx) => tx,
                Err(e) => return Err(WriteProxyErr::MysqlErr(e)),
            };

            let column_names = table_info.column_names.as_ref().unwrap();

            let params : Vec<(String, mysql::Value)> = column_names.iter()
                .enumerate()
                .map(|(i, column_name)| (column_name.clone(), records[i].clone()))
                .collect();
            for policy in startup_policies {
                if let Err(e) = tx.exec_drop(policy, &params) {
                    return Err(WriteProxyErr::MysqlErr(e));
                }
            }

            let insert_predicate = format!("INSERT INTO {} ({}) SELECT {} {}",
                table_name,
                column_names.join(","),
                column_names.iter().map(|col| format!(":{}", col)).collect::<Vec<String>>().join(","),
                predicate);

            if let Err(e) = tx.exec_drop(insert_predicate, params) {
                return Err(WriteProxyErr::MysqlErr(e));
            }

            match tx.query_first::<u8, _>("SELECT row_count();") {
                Ok(Some(num)) if num == 0 => return Err(WriteProxyErr::PolicyNotInserted),
                Ok(Some(_)) => {}, // Insert worked, move on.
                Ok(None) => unreachable!("SELECT row_count() must return something!"),
                Err(e) => return Err(WriteProxyErr::MysqlErr(e)),
            };

            if let Err(e) = tx.commit() {
                return Err(WriteProxyErr::MysqlErr(e));
            }
        }
        // TODO: propagate write to Noria!

        Ok(())
    }
}