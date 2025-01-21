use chrono::{DateTime, Local};
use clap::Args;
use rusqlite::Connection;
use xdg::BaseDirectories;
use crate::{create_table, schema, select_jobs, insert_job};

#[derive(Args, Debug)]
pub struct List {}

impl List {
    pub fn run(&self, base: BaseDirectories) -> anyhow::Result<()> {
        let data_home = base.get_data_home();
        let db_path = data_home.join("rat.db");
        let conn = Connection::open(db_path)?;

        create_table(&conn)?;

        let jobs = select_jobs(&conn)?;

        println!("ID\tName\tScript\tRun At");
        for job in jobs {
            println!("{}\t{}\t{}\t{}", job.id, job.name.unwrap_or("".to_string()), job.script, job.run_at);
        }
        Ok(())
    }
}

#[derive(Args, Debug)]
pub struct Add {
    #[clap(short, long)]
    pub name: Option<String>,

    pub run_at: DateTime<Local>,
    pub script: String,
}

impl Add {
    pub fn run(&self, base: BaseDirectories) -> anyhow::Result<()> {
        let data_home = base.get_data_home();
        let db_path = data_home.join("rat.db");
        let conn = Connection::open(db_path)?;

        let job = schema::Job {
            id: 0,
            name: self.name.clone(),
            script: self.script.clone(),
            run_at: self.run_at.to_utc(),
        };
        insert_job(&conn, &job)?;

        Ok(())
    }
}