use std::path::PathBuf;
use std::thread;
use std::time::Duration;

use anyhow::Context;
use chrono::{DateTime, Local, Utc};
use clap::Args;
use prettytable::{format, row, Table};

use crate::{
    schema::{self, JobBuilder},
    JobManager,
};

#[derive(Args, Debug)]
pub struct List {}

impl List {
    pub fn run(&self, job_manager: JobManager) -> anyhow::Result<()> {
        let jobs = job_manager.get_all_jobs()?;

        let mut table = Table::new();
        table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);

        table.set_titles(row!["ID", "Name", "State", "Script", "Run At"]);
        for job in jobs {
            let state = match job.state {
                schema::JobState::Done => {
                    let result = job_manager.get_result(&job)?;
                    assert!(result.is_some(), "job state is Done but result is None");

                    let result = result.unwrap();
                    assert!(
                        result.status.is_some(),
                        "job state is Done but result status is None"
                    );

                    let status = result.status.unwrap();
                    format!("({})", status)
                }
                _ => "".to_string(),
            };
            table.add_row(row![
                job.id,
                job.name.unwrap_or("".to_string()),
                format!("{}{}", job.state, state),
                job.script,
                job.run_at,
            ]);
        }
        table.printstd();
        Ok(())
    }
}

#[derive(Args, Debug)]
pub struct Add {
    #[clap(short, long)]
    pub name: Option<String>,

    pub run_at: DateTime<Local>,
    pub script: String,

    pub cwd: Option<PathBuf>,
}

impl Add {
    pub fn run(&self, job_manager: JobManager) -> anyhow::Result<()> {
        let mut job_manager = job_manager;
        let cwd = std::env::current_dir()?;
        let cwd = self.cwd.clone().unwrap_or(cwd);

        let job_builder = JobBuilder::new()
            .state(schema::JobState::Queued)
            .script(self.script.clone())
            .run_at(self.run_at.to_utc())
            .cwd(cwd);
        let job_builder = if let Some(name) = &self.name {
            job_builder.name(name.clone())
        } else {
            job_builder
        };

        let job = job_builder.build();
        let job = job_manager.enqueue(job)?;
        println!("add job {}", job.id);
        Ok(())
    }
}

#[derive(Args, Debug)]
pub struct Delete {
    pub job_id: i64,
}

impl Delete {
    pub fn run(&self, job_manager: JobManager) -> anyhow::Result<()> {
        let mut job_manager = job_manager;

        let Some(job) = job_manager
            .get_job(self.job_id.into())
            .context("failed to get job")?
        else {
            eprintln!("job {} not found", self.job_id);
            std::process::exit(1);
        };
        let _ = job_manager.delete(&job).context("failed to delete job")?;
        Ok(())
    }
}

#[derive(Args, Debug)]
pub struct Run {
    #[clap(short, long, value_parser = humantime::parse_duration)]
    stop_early: Option<Duration>,
}

impl Run {
    pub fn run(&self, job_manager: JobManager) -> anyhow::Result<()> {
        let mut job_manager = job_manager;

        let interval = Duration::from_secs(1);
        log::info!("interval: {:?}", interval);
        loop {
            self.run_job(&mut job_manager, interval)?;
        }
    }

    fn run_job(&self, job_manager: &mut JobManager, interval: Duration) -> anyhow::Result<()> {
        let Some(job) = job_manager.dequeue().context("failed to dequeue job")? else {
            log::debug!("no jobs");
            thread::sleep(interval);
            return Ok(());
        };
        log::info!("fetched job:{}", job.id);

        let job_id = job.id;
        let wait_time = job.run_at - Utc::now();
        if wait_time.num_seconds() > 0 {
            log::info!(
                "wait {} seconds to start job:{}",
                wait_time.num_seconds(),
                job_id
            );
            let wait_time = wait_time.to_std().unwrap();
            log::debug!(
                "wait {} seconds to start job:{}",
                wait_time.as_secs(),
                job_id
            );

            if wait_time > interval {
                log::debug!("sleep {} seconds", interval.as_secs());
                thread::sleep(interval);
                return Ok(());
            }
            log::debug!("sleep {} seconds", wait_time.as_secs());
            thread::sleep(wait_time);
        }

        let mut job = job;
        job.mark_running()?;
        log::info!("update job state to Running:{}", job_id);

        log::info!("start job:{}", job_id);
        println!("start job:{}", job_id);
        let output = std::process::Command::new("/bin/sh")
            .arg("-c")
            .arg(&job.script)
            .current_dir(&job.cwd)
            .output()?;

        let mut job_result = schema::JobResult::new(job.id);
        job_result.status = output.status.code().map(|c| c as i16);
        job_result.stdout = String::from_utf8_lossy(&output.stdout).to_string();
        job_result.stderr = String::from_utf8_lossy(&output.stderr).to_string();

        let _ = job.save_job_result(job_result)?;
        log::info!("insert job result:{}", job_id);
        log::info!("update job state to Done:{}", job_id);
        println!("done job:{}", job_id);

        Ok(())
    }
}

#[derive(Args, Debug)]
pub struct Log {
    pub job_id: i64,
}

impl Log {
    pub fn run(&self, job_manager: JobManager) -> anyhow::Result<()> {
        let Some(job) = job_manager
            .get_job(self.job_id.into())
            .context("failed to get job")?
        else {
            eprintln!("job {} not found", self.job_id);
            std::process::exit(1);
        };

        let Some(result) = job_manager
            .get_result(&job)
            .context("failed to get job result")?
        else {
            eprintln!("job {} result not found", job.id);
            std::process::exit(1);
        };
        print!("{}", result.stdout);
        eprint!("{}", result.stderr);

        Ok(())
    }
}
