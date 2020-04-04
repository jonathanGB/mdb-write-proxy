use mysql::prelude::*;
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::{Parser, ParserError};
use std::collections::HashMap;
use std::fs;

type Message = (u64, u64, String);

fn main() {
    let policy_content = fs::read_to_string("src/twitter-write-policies.json").unwrap();
    let policy_content_json : serde_json::Value = serde_json::from_str(&policy_content).unwrap();
    let pool = WriteProxy::default_pool().unwrap();
    let policies = WriteProxy::parse_policy_content(policy_content_json);
    println!("{:?}", policies);
    let mut write_proxy = WriteProxy::new(pool, policies);

    let messages_sql = r"
    CREATE TABLE Messages(
        sender_id int,
        sendee_id int,
        content VARCHAR(100)
    );
    ";

    write_proxy.extend_recipe(messages_sql);
    let insert_res = write_proxy.insert("Messages", vec![1.into(), 4.into(), "message from #1 to #4".into()]);
    println!("{:?}", insert_res);
}

struct WriteProxy {
    pool: mysql::Pool,
    tables: HashMap<String, TableInfo>,
}

#[derive(Debug)]
enum WriteProxyErr<'a> {
    MysqlErr(mysql::Error),
    PolicyErr(String),
    MissingTable(&'a str),
    ParserErr(ParserError),
}

#[derive(Debug)]
struct TableInfo {
    policies: Option<Policies>,
    columns: Option<(HashMap<String, usize>, Vec<String>)>,
}
type Policies = (Vec<(String, QueryParams)>, (String, QueryParams));
type QueryParams = Vec<String>;

impl WriteProxy {
    pub fn default_pool() -> Result<mysql::Pool, mysql::Error> {
        mysql::Pool::new("mysql://root:@localhost:3306/mdb")
    }

    pub fn parse_policy_content(policies: serde_json::Value) -> HashMap<String, TableInfo> {
        if !policies.is_object() {
            unreachable!("Policy file should be a JSON object!");
        }

        let mut tables = HashMap::default();
        for (table, table_obj) in policies.as_object().unwrap() {
            if !table_obj.is_object() {
                unreachable!(format!("Entry {} should be an object!", table));
            }

            let table_policies = table_obj.as_object().unwrap();
            let startup_policies = table_policies.get("startup");
            let predicate = table_policies.get("predicate");

            if startup_policies.is_none() {
                unreachable!(format!("Entry {} has no `startup` set!", table));
            }
            if predicate.is_none() {
                unreachable!(format!("Entry {} has no `predicate` set!", table));
            }

            let startup_policies = startup_policies.unwrap();
            let predicate = predicate.unwrap();

            if !startup_policies.is_array() {
                unreachable!(format!("Entry {} has a `startup` set, but it should be an array!", table));
            }

            if !predicate.is_string() {
                unreachable!(format!("Entry {} has a `predicate` set, but it should be a string!", table));
            }

            let policies = startup_policies.as_array().unwrap().iter().enumerate().map(|(i, startup_policy)| {
                if !startup_policy.is_string() {
                    unreachable!(format!("Entry {} `startup` #{} should be a string!", table, i));
                }

                let policy = startup_policy.as_str().unwrap().to_string();
                let policy_query_params = Self::get_query_params(&policy);
                (policy, policy_query_params)
            }).collect();
            let predicate = predicate.as_str().unwrap().to_string();
            let predicate_query_params = Self::get_query_params(&predicate);
            let predicate_query_params = (predicate, predicate_query_params);

            tables.insert(table.clone(), TableInfo{
                policies: Some((policies, predicate_query_params)),
                columns: None,
            });
        }

        tables
    }

    fn get_query_params(query: &str) -> QueryParams {
        use mysql_common::named_params::parse_named_params;
        parse_named_params(query).unwrap().0.unwrap_or(vec![])
    }

    pub fn new(pool: mysql::Pool, tables: HashMap<String, TableInfo>) -> Self {
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
        use sqlparser::ast::Statement::CreateTable;

        let sql_dialect = MySqlDialect{};
        let query = Parser::parse_sql(&sql_dialect, stmt.to_string());
        if query.is_err() {
            return Err(WriteProxyErr::ParserErr(query.unwrap_err().clone()));
        }
        let query = &query.unwrap()[0];

        // If we have a `Create Table` statement, propagate it to our SQL table.
        // Also, keep track of the columns, so that subsequent INSERT/UPDATEs values
        // can be interpolated.
        if let CreateTable{name, columns, ..} = query {
            let mut conn = self.pool.get_conn().unwrap();
            let create_table_res : Result<(), _> = conn.query_drop(stmt);
            if create_table_res.is_err() {
                return Err(WriteProxyErr::MysqlErr(create_table_res.unwrap_err()));
            }

            // TODO: we only remember the first part of the table name.
            // If we were given `my_db.my_table`, this would not work properly.
            // This should not be a problem, because Noria doesn't have such notation.
            let table_name = name.0[0].to_string();
            let column_names = columns.iter().map(|column| column.name.clone()).collect::<Vec<String>>();
            let columns : HashMap<String, usize> = column_names.iter().enumerate().map(|(i, column_name)| (column_name.clone(), i)).collect();
            let columns = (columns, column_names);

            self.tables.entry(table_name)
                .or_insert(TableInfo{
                    policies: None,
                    columns: None,
                })
                .columns = Some(columns);
        }

        // TODO: Propagate to Noria!
        Ok(())
    }

    pub fn insert<'a>(&'a mut self, table_name: &'a str, mut records: Vec<mysql::Value>) -> Result<u64, WriteProxyErr<'a>> {
        let table = self.tables.get(table_name);
        if table.is_none() || table.unwrap().columns.is_none() {
            return Err(WriteProxyErr::MissingTable(table_name));
        }

        let conn = self.pool.get_conn();
        if conn.is_err() {
            return Err(WriteProxyErr::MysqlErr(conn.unwrap_err()));
        }
        let mut conn = conn.unwrap();
        let tx = conn.start_transaction(Default::default());
        if tx.is_err() {
            return Err(WriteProxyErr::MysqlErr(tx.unwrap_err()));
        }
        let mut tx = tx.unwrap();

        let table_info = table.unwrap();
        let columns = table_info.columns.as_ref().unwrap();
        match table_info.policies {
            Some((ref policies, ref predicate)) => {
                // Apply all policies.
                for (policy, params) in policies {
                    let params_values = params.iter().map(|param|
                        match columns.0.get(param) {
                            Some(param_idx) => records[*param_idx].clone(),
                            None => unreachable!(), //return Err(WriteProxyErr::MissingColumn(param)),
                        }
                    )
                    .collect::<Vec<mysql::Value>>();

                    let tx_res : Result<(), _> = tx.exec_drop(policy, params_values);
                    println!("{:?}", tx_res);
                }

                // Apply the insertion predicate.
                let (predicate, predicate_params) = predicate;
                let params_values = predicate_params.iter().map(|param|
                    match columns.0.get(param) {
                        Some(param_idx) => records[*param_idx].clone(),
                        None => unreachable!(), //return Err(WriteProxyErr::MissingColumn(param)),
                    }
                )
                .collect::<Vec<mysql::Value>>();

                let insert_predicate = format!("INSERT INTO {} ({}) SELECT {} {}",
                    table_name,
                    columns.1.join(","),
                    columns.1.iter().map(|col| format!(":{}", col)).collect::<Vec<String>>().join(","),
                    predicate);

                records.extend(params_values);

                let tx_res : Result<(), _> = tx.exec_drop(insert_predicate, records);
                println!("{:?}", tx_res);
            }
            None => {
                // TODO: Just propagate write to Noria!
            }
        };

        let row_count : Result<Option<i64>, _> = tx.query_first("SELECT row_count();");
        if row_count.is_err() {
            return Err(WriteProxyErr::MysqlErr(row_count.unwrap_err()));
        }
        let tx_res = tx.commit();
        if tx_res.is_err() {
            return Err(WriteProxyErr::MysqlErr(tx_res.unwrap_err()));
        }

        if row_count.unwrap().unwrap() == 0 {
            return Ok(0);
        }

        // TODO: propagate write to Noria!

        Ok(1)
    }
}