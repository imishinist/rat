use crate::schema::{Job, JobResult, JobState};
use rusqlite::{params, Connection};
use std::path::{Path, PathBuf};

pub mod commands;
pub mod schema;

pub type Result<T> = anyhow::Result<T>;

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

pub fn create_table(conn: &Connection) -> anyhow::Result<()> {
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

pub fn insert_job(conn: &Connection, job: &Job) -> Result<()> {
    conn.execute(
        "INSERT INTO jobs (name, state, script, run_at, cwd) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            job.name,
            job.state,
            job.script,
            job.run_at,
            path_to_bytes(&job.cwd)
        ],
    )?;
    Ok(())
}

pub fn update_job_state(conn: &Connection, job: &Job, state: JobState) -> Result<()> {
    conn.execute(
        "UPDATE jobs SET state = ?1 WHERE id = ?2",
        params![state, job.id],
    )?;
    Ok(())
}

pub fn insert_job_result(conn: &Connection, job_result: &JobResult) -> Result<()> {
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

pub fn get_job(conn: &Connection, job_id: u16) -> Result<Option<Job>> {
    let mut stmt =
        conn.prepare("SELECT id,name,state,script,run_at,cwd FROM jobs WHERE id = ?1")?;
    let job = stmt
        .query_map(params![job_id], |row| {
            Ok(Job {
                id: row.get(0)?,
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

pub fn select_all_jobs(conn: &Connection) -> Result<Vec<Job>> {
    select_jobs(conn, None)
}

pub fn select_queued_jobs(conn: &Connection) -> Result<Vec<Job>> {
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
        Ok(Job {
            id: row.get(0)?,
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

pub fn get_job_result(conn: &Connection, job: &Job) -> Result<Option<JobResult>> {
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

pub fn select_job_results(conn: &Connection) -> Result<Vec<JobResult>> {
    let mut stmt = conn.prepare("SELECT id,job_id,status,stdout,stderr FROM job_results")?;
    let job_results = stmt.query_map(params![], |row| {
        Ok(JobResult {
            id: row.get(0)?,
            job_id: row.get(1)?,
            status: row.get(2)?,
            stdout: row.get(3)?,
            stderr: row.get(4)?,
        })
    })?;
    let mut result = Vec::new();
    for job_result in job_results {
        result.push(job_result?);
    }
    Ok(result)
}

pub fn delete_job(conn: &mut Connection, job: &Job) -> Result<()> {
    let tx = conn.transaction()?;
    tx.execute("DELETE FROM job_results WHERE job_id = ?1", params![job.id])?;
    tx.execute("DELETE FROM jobs WHERE id = ?1", params![job.id])?;
    tx.commit()?;

    Ok(())
}
