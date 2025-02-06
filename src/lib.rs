use std::path::{Path, PathBuf};

use anyhow::Context;
use rusqlite::Connection;

use crate::schema::{Job, JobResult, JobState, ID};

pub mod commands;
mod db;
pub mod schema;

pub type Result<T> = anyhow::Result<T>;

pub struct JobManager {
    conn: Connection,
}

impl JobManager {
    pub fn new<P: AsRef<Path>>(data_home: P) -> Result<Self> {
        let data_home = data_home.as_ref();
        let db_path = data_home.join("rat.db");
        let conn = Connection::open(db_path)?;
        let _ = db::create_table(&conn).context("Failed to create table")?;

        Ok(JobManager { conn })
    }

    pub fn dequeue(&mut self) -> Result<Option<Job>> {
        let mut jobs = db::select_queued_jobs(&self.conn)?;
        jobs.sort_by(|a, b| a.run_at.cmp(&b.run_at).reverse());
        Ok(jobs.pop())
    }

    pub fn enqueue(&mut self, mut job: Job) -> Result<Job> {
        let job_id = db::insert_job(&mut self.conn, &job)?;
        job.id = job_id.into();
        Ok(job)
    }

    pub fn save_job_result(&mut self, job_result: JobResult) -> Result<JobResult> {
        let job_result_id = db::insert_job_result(&mut self.conn, &job_result)?;
        Ok(JobResult {
            id: job_result_id.into(),
            ..job_result
        })
    }

    pub fn get_job(&self, job_id: ID) -> Result<Option<Job>> {
        db::select_job(&self.conn, job_id)
    }

    pub fn update_job_state(&mut self, job: &Job, state: JobState) -> Result<()> {
        db::update_job_state(&mut self.conn, job, state)
    }

    pub fn delete(&mut self, job: &Job) -> Result<()> {
        if job.state == JobState::Doing {
            return Err(anyhow::anyhow!(
                "cannot delete a job #{} that is currently running",
                job.id
            ));
        }
        db::delete_job(&mut self.conn, job)
    }

    pub fn get_all_jobs(&self) -> Result<Vec<Job>> {
        db::select_all_jobs(&self.conn)
    }

    pub fn get_result(&self, job: &Job) -> Result<Option<JobResult>> {
        db::get_job_result(&self.conn, job)
    }
}

#[cfg(unix)]
fn path_to_bytes<P: AsRef<Path>>(path: P) -> Vec<u8> {
    use std::os::unix::ffi::OsStrExt;
    path.as_ref().as_os_str().as_bytes().to_vec()
}

#[cfg(unix)]
fn bytes_to_path<S: AsRef<[u8]>>(buf: S) -> PathBuf {
    use std::os::unix::ffi::OsStrExt;
    PathBuf::from(std::ffi::OsStr::from_bytes(buf.as_ref()))
}

#[cfg(windows)]
fn path_to_bytes<P: AsRef<Path>>(path: P) -> Vec<u8> {
    // not tested
    path.as_ref()
        .as_os_str()
        .encode_wide()
        .map(|c| c.to_le_bytes())
        .flatten()
        .collect()
}

#[cfg(windows)]
fn bytes_to_path(buf: &[u8]) -> PathBuf {
    // not tested
    use std::ffi::OsString;
    use std::os::windows::ffi::OsStringExt;
    OsString::from_wide(
        buf.chunks_exact(2)
            .map(|c| u16::from_le_bytes([c[0], c[1]]))
            .collect::<Vec<u16>>(),
    )
    .into()
}
