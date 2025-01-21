use chrono::{DateTime, Utc};
use std::cmp::Ordering;
use std::fmt;
use std::fmt::{Display, Formatter};
use rusqlite::ToSql;
use rusqlite::types::{FromSql, FromSqlResult, ToSqlOutput, ValueRef};

#[repr(C)]
#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum JobState {
    Queued = 0,
    Done = 1,
    Canceled = 2,
}

impl Display for JobState {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            JobState::Queued => write!(f, "Queued"),
            JobState::Done => write!(f, "Done"),
            JobState::Canceled => write!(f, "Canceled"),
        }
    }
}

impl FromSql for JobState {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value.as_i64()? {
            0 => Ok(JobState::Queued),
            1 => Ok(JobState::Done),
            2 => Ok(JobState::Canceled),
            _ => Err(rusqlite::types::FromSqlError::InvalidType),
        }
    }
}

impl ToSql for JobState {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        use rusqlite::types::Value::Integer;
        match *self {
            JobState::Queued => Ok(ToSqlOutput::Owned(Integer(0))),
            JobState::Done => Ok(ToSqlOutput::Owned(Integer(1))),
            JobState::Canceled => Ok(ToSqlOutput::Owned(Integer(2))),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct Job {
    pub id: u16,
    pub name: Option<String>,
    pub state: JobState,
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
