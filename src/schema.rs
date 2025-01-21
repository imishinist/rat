use chrono::{DateTime, Utc};
use std::cmp::Ordering;

#[derive(Debug, Eq, PartialEq)]
pub struct Job {
    pub id: u16,
    pub name: Option<String>,
    pub script: String,
    pub run_at: DateTime<Utc>,
}

impl PartialOrd<Self> for Job {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Job {
    fn cmp(&self, other: &Self) -> Ordering {
        self.run_at.cmp(&other.run_at)
    }
}

#[derive(Debug)]
pub struct JobResult {
    pub id: u16,
    pub job_id: u16,
    pub status: Option<i16>,

    pub stdout: String,
    pub stderr: String,
}

impl JobResult {
    pub fn new(job_id: u16) -> Self {
        Self {
            id: 0,
            job_id,
            status: None,
            stdout: String::new(),
            stderr: String::new(),
        }
    }
}
