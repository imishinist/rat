use chrono::{DateTime, Utc};

#[derive(Debug)]
pub struct Job {
    pub id: u16,
    pub name: Option<String>,
    pub script: String,
    pub run_at: DateTime<Utc>,
}

#[derive(Debug)]
pub struct JobResult {
    pub id: u16,
    pub job_id: u16,
    pub status: u16,
    pub result: String,
}
