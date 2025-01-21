use crate::{create_table, insert_job, insert_job_result, schema, select_all_jobs, select_queued_jobs};
use chrono::{DateTime, Local};
use clap::Args;
use rusqlite::Connection;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::thread;
use std::time::Duration;
use xdg::BaseDirectories;

#[derive(Args, Debug)]
pub struct List {}

impl List {
    pub fn run(&self, base: BaseDirectories) -> anyhow::Result<()> {
        let data_home = base.get_data_home();
        let db_path = data_home.join("rat.db");
        let conn = Connection::open(db_path)?;

        create_table(&conn)?;

        let jobs = select_all_jobs(&conn)?;
        println!("ID\tName\tState\tScript\tRun At");
        for job in jobs {
            println!(
                "{}\t{}\t{}\t{}\t{}",
                job.id,
                job.name.unwrap_or("".to_string()),
                job.state,
                job.script,
                job.run_at
            );
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
        create_table(&conn)?;

        let job = schema::Job {
            id: 0,
            name: self.name.clone(),
            state: schema::JobState::Queued,
            script: self.script.clone(),
            run_at: self.run_at.to_utc(),
        };
        insert_job(&conn, &job)?;

        Ok(())
    }
}

#[derive(Args, Debug)]
pub struct Run {
    #[clap(short, long, value_parser = humantime::parse_duration)]
    stop_early: Option<Duration>,
}

impl Run {
    pub fn run(&self, base: BaseDirectories) -> anyhow::Result<()> {
        let data_home = base.get_data_home();
        let db_path = data_home.join("rat.db");
        let conn = Connection::open(db_path)?;
        create_table(&conn)?;

        let jobs = select_queued_jobs(&conn)?;
        let mut pq = BinaryHeap::new();
        for job in jobs {
            pq.push(Reverse(job));
        }

        let mut stop_early = self.stop_early;

        // sequentially run jobs
        while let Some(job) = pq.pop() {
            let job = job.0;

            let job_id = format!("#{}", job.id.to_string());
            let job_name = job.name.unwrap_or(job_id.clone());
            let wait_time = job.run_at - chrono::Utc::now();

            if wait_time.num_seconds() > 0 {
                let wait_time = wait_time.to_std()?;
                if stop_early.is_some() {
                    let stop_early = stop_early.take().unwrap();
                    if wait_time > stop_early {
                        println!("Stopping early");
                        break;
                    }
                }

                println!(
                    "Job {} is due in {}",
                    job_id,
                    humantime::format_duration(wait_time)
                );
                thread::sleep(wait_time);
            }

            println!("Started job:{}", job_name);
            let output = std::process::Command::new("/bin/sh")
                .arg("-c")
                .arg(&job.script)
                .output()?;

            let mut job_result = schema::JobResult::new(job.id);
            job_result.status = output.status.code().map(|c| c as i16);
            job_result.stdout = String::from_utf8_lossy(&output.stdout).to_string();
            job_result.stderr = String::from_utf8_lossy(&output.stderr).to_string();
            insert_job_result(&conn, schema::JobState::Done, &job_result)?;
            println!("done job:{}", job_name);
        }

        Ok(())
    }
}
