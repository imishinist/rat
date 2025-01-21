use rusqlite::{params, Connection};

pub mod commands;
pub mod schema;

pub type Result<T> = anyhow::Result<T>;

pub fn create_table(conn: &Connection) -> anyhow::Result<()> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS jobs (
                  id              INTEGER PRIMARY KEY,
                  name            TEXT,
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
                  result          TEXT NOT NULL
             )",
        [],
    )?;
    Ok(())
}

pub fn insert_job(conn: &Connection, job: &schema::Job) -> Result<()> {
    conn.execute(
        "INSERT INTO jobs (name, script, run_at) VALUES (?1, ?2, ?3)",
        params![job.name, job.script, job.run_at],
    )?;
    Ok(())
}

pub fn select_jobs(conn: &Connection) -> Result<Vec<schema::Job>> {
    let mut stmt = conn.prepare("SELECT id,name,script,run_at FROM jobs")?;
    let jobs = stmt.query_map(params![], |row| {
        Ok(schema::Job {
            id: row.get(0)?,
            name: row.get(1)?,
            script: row.get(2)?,
            run_at: row.get(3)?,
        })
    })?;
    let mut result = Vec::new();
    for job in jobs {
        result.push(job?);
    }
    Ok(result)
}

pub fn select_job_results(conn: &Connection) -> Result<Vec<schema::JobResult>> {
    let mut stmt = conn.prepare("SELECT id,job_id,status,result FROM job_results")?;
    let job_results = stmt.query_map(params![], |row| {
        Ok(schema::JobResult {
            id: row.get(0)?,
            job_id: row.get(1)?,
            status: row.get(2)?,
            result: row.get(3)?,
        })
    })?;
    let mut result = Vec::new();
    for job_result in job_results {
        result.push(job_result?);
    }
    Ok(result)
}
