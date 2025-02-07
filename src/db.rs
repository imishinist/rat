use rusqlite::{params, Connection};

use crate::schema::{Job, JobResult, JobState, ID};
use crate::{bytes_to_path, path_to_bytes, Result};

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

pub fn select_queued_jobs(conn: &Connection) -> Result<Vec<Job>> {
    select_jobs(conn, Some(JobState::Queued))
}

pub fn insert_job(conn: &mut Connection, job: &Job) -> Result<ID> {
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

    Ok(id.into())
}

pub fn insert_job_result(conn: &mut Connection, job_result: &JobResult) -> Result<ID> {
    let tx = conn.transaction()?;

    tx.execute(
        "UPDATE jobs SET state = ?1 WHERE id = ?2",
        params![JobState::Done, job_result.job_id],
    )?;
    tx.execute(
        "INSERT INTO job_results (job_id, status, stdout, stderr) VALUES (?1, ?2, ?3, ?4)",
        params![
            job_result.job_id,
            job_result.status,
            job_result.stdout,
            job_result.stderr
        ],
    )?;
    let id = tx.last_insert_rowid();
    tx.commit()?;

    Ok(id.into())
}

pub fn select_job(conn: &Connection, job_id: ID) -> Result<Option<Job>> {
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

pub fn update_job_state(conn: &mut Connection, job: &Job, state: JobState) -> Result<()> {
    let tx = conn.transaction()?;
    tx.execute(
        "UPDATE jobs SET state = ?1 WHERE id = ?2",
        params![state, job.id],
    )?;
    tx.commit()?;
    Ok(())
}

pub fn delete_job(conn: &mut Connection, job: &Job) -> Result<()> {
    let tx = conn.transaction()?;
    tx.execute("DELETE FROM job_results WHERE job_id = ?1", params![job.id])?;
    tx.execute("DELETE FROM jobs WHERE id = ?1", params![job.id])?;
    tx.commit()?;

    Ok(())
}

pub fn select_all_jobs(conn: &Connection) -> Result<Vec<Job>> {
    select_jobs(conn, None)
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
