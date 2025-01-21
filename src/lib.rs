use crate::schema::{Job, JobResult, JobState};
use rusqlite::{params, Connection};

pub mod commands;
pub mod schema;

pub type Result<T> = anyhow::Result<T>;

pub fn create_table(conn: &Connection) -> anyhow::Result<()> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS jobs (
                  id              INTEGER PRIMARY KEY,
                  name            TEXT,
                  state           INTEGER NOT NULL,
                  script          TEXT NOT NULL,
                  run_at          TEXT NOT NULL
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
        "INSERT INTO jobs (name, state, script, run_at) VALUES (?1, ?2, ?3, ?4)",
        params![job.name, job.state, job.script, job.run_at],
    )?;
    Ok(())
}

pub fn insert_job_result(conn: &Connection, state: JobState, job_result: &JobResult) -> Result<()> {
    conn.execute(
        "INSERT INTO job_results (job_id, status, stdout, stderr) VALUES (?1, ?2, ?3, ?4)",
        params![
            job_result.job_id,
            job_result.status,
            job_result.stdout,
            job_result.stderr
        ],
    )?;
    conn.execute(
        "UPDATE jobs SET state = ?1 WHERE id = ?2",
        params![state, job_result.job_id],
    )?;

    Ok(())
}

pub fn get_job(conn: &Connection, job_id: u16) -> Result<Option<Job>> {
    let mut stmt = conn.prepare("SELECT id,name,state,script,run_at FROM jobs WHERE id = ?1")?;
    let job = stmt
        .query_map(params![job_id], |row| {
            Ok(Job {
                id: row.get(0)?,
                name: row.get(1)?,
                state: row.get(2)?,
                script: row.get(3)?,
                run_at: row.get(4)?,
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
            conn.prepare("SELECT id,name,state,script,run_at FROM jobs WHERE state = ?1")?,
            params![state.clone()],
        ),
        None => (
            conn.prepare("SELECT id,name,state,script,run_at FROM jobs")?,
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
