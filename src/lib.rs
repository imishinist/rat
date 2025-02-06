use std::path::{Path, PathBuf};

use anyhow::Context;
use rusqlite::{params, Connection};

use crate::schema::{Job, JobResult, JobState};

pub mod commands;
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
        let _ = create_table(&conn).context("Failed to create table")?;

        Ok(JobManager { conn })
    }

    pub fn dequeue(&mut self) -> Result<Option<Job>> {
        let mut jobs = select_queued_jobs(&self.conn)?;
        jobs.sort_by(|a, b| a.run_at.cmp(&b.run_at).reverse());
        Ok(jobs.pop())
    }

    pub fn enqueue(&mut self, mut job: Job) -> Result<Job> {
        let job_id = insert_job(&mut self.conn, &job)?;
        job.id = job_id.into();
        Ok(job)
    }

    pub fn save_job_result(&mut self, job_result: &JobResult) -> Result<()> {
        insert_job_result(&self.conn, job_result)
    }

    pub fn get_job(&self, job_id: i64) -> Result<Option<Job>> {
        get_job(&self.conn, job_id)
    }

    pub fn update_job_state(&mut self, job: &Job, state: JobState) -> Result<()> {
        update_job_state(&self.conn, job, state)
    }

    pub fn delete(&mut self, job: &Job) -> Result<()> {
        let _ = delete_job(&mut self.conn, job)?;
        if job.state == JobState::Doing {
            return Err(anyhow::anyhow!(
                "cannot delete a job #{} that is currently running",
                job.id
            ));
        }
        Ok(())
    }

    pub fn get_all_jobs(&self) -> Result<Vec<Job>> {
        select_all_jobs(&self.conn)
    }

    pub fn get_result(&self, job: &Job) -> Result<Option<JobResult>> {
        get_job_result(&self.conn, job)
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

fn create_table(conn: &Connection) -> anyhow::Result<()> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS jobs (
                  id              INTEGER PRIMARY KEY,
                  name            TEXT,
                  state           INTEGER NOT NULL,
                  script          TEXT NOT NULL,
                  run_at          TEXT NOT NULL,
                  cwd             BLOB NOT NULL
             )",
        [],
    )?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS job_results (
                  id              INTEGER PRIMARY KEY,
                  job_id          INTEGER NOT NULL,
                  status          INTEGER NOT NULL,
                  stdout          TEXT NOT NULL,
                  stderr          TEXT NOT NULL
             )",
        [],
    )?;
    Ok(())
}

fn insert_job(conn: &mut Connection, job: &Job) -> Result<i64> {
    let tx = conn.transaction()?;

    tx.execute(
        "INSERT INTO jobs (name, state, script, run_at, cwd) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            job.name,
            job.state,
            job.script,
            job.run_at,
            path_to_bytes(&job.cwd)
        ],
    )?;
    let id = tx.last_insert_rowid();
    tx.commit()?;

    Ok(id)
}

fn update_job_state(conn: &Connection, job: &Job, state: JobState) -> Result<()> {
    conn.execute(
        "UPDATE jobs SET state = ?1 WHERE id = ?2",
        params![state, job.id],
    )?;
    Ok(())
}

fn insert_job_result(conn: &Connection, job_result: &JobResult) -> Result<()> {
    conn.execute(
        "INSERT INTO job_results (job_id, status, stdout, stderr) VALUES (?1, ?2, ?3, ?4)",
        params![
            job_result.job_id,
            job_result.status,
            job_result.stdout,
            job_result.stderr
        ],
    )?;

    Ok(())
}

fn get_job(conn: &Connection, job_id: i64) -> Result<Option<Job>> {
    let mut stmt =
        conn.prepare("SELECT id,name,state,script,run_at,cwd FROM jobs WHERE id = ?1")?;
    let job = stmt
        .query_map(params![job_id], |row| {
            let id: i64 = row.get(0)?;
            Ok(Job {
                id: id.into(),
                name: row.get(1)?,
                state: row.get(2)?,
                script: row.get(3)?,
                run_at: row.get(4)?,
                cwd: bytes_to_path(row.get::<_, Vec<u8>>(5)?),
            })
        })?
        .next()
        .transpose()?;
    Ok(job)
}

fn select_all_jobs(conn: &Connection) -> Result<Vec<Job>> {
    select_jobs(conn, None)
}

fn select_queued_jobs(conn: &Connection) -> Result<Vec<Job>> {
    select_jobs(conn, Some(JobState::Queued))
}

fn select_jobs(conn: &Connection, state: Option<JobState>) -> Result<Vec<Job>> {
    let (mut stmt, params) = match state {
        Some(state) => (
            conn.prepare("SELECT id,name,state,script,run_at,cwd FROM jobs WHERE state = ?1")?,
            params![state.clone()],
        ),
        None => (
            conn.prepare("SELECT id,name,state,script,run_at,cwd FROM jobs")?,
            params![],
        ),
    };
    let jobs = stmt.query_map(params, |row| {
        let id: i64 = row.get(0)?;
        Ok(Job {
            id: id.into(),
            name: row.get(1)?,
            state: row.get(2)?,
            script: row.get(3)?,
            run_at: row.get(4)?,
            cwd: bytes_to_path(row.get::<_, Vec<u8>>(5)?),
        })
    })?;
    let mut result = Vec::new();
    for job in jobs {
        result.push(job?);
    }
    Ok(result)
}

fn get_job_result(conn: &Connection, job: &Job) -> Result<Option<JobResult>> {
    let mut stmt =
        conn.prepare("SELECT id,status,stdout,stderr FROM job_results WHERE job_id = ?1")?;
    let job_result = stmt
        .query_map(params![job.id], |row| {
            Ok(JobResult {
                id: row.get(0)?,
                job_id: job.id,
                status: row.get(1)?,
                stdout: row.get(2)?,
                stderr: row.get(3)?,
            })
        })?
        .next()
        .transpose()?;
    Ok(job_result)
}

fn delete_job(conn: &mut Connection, job: &Job) -> Result<()> {
    let tx = conn.transaction()?;
    tx.execute("DELETE FROM job_results WHERE job_id = ?1", params![job.id])?;
    tx.execute("DELETE FROM jobs WHERE id = ?1", params![job.id])?;
    tx.commit()?;

    Ok(())
}
