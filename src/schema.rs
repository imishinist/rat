use std::cmp::Ordering;
use std::fmt::{Display, Formatter};
use std::marker::PhantomData;
use std::ops::Deref;
use std::path::PathBuf;
use std::{env, fmt};

use chrono::{DateTime, Utc};
use rusqlite::types::{FromSql, FromSqlResult, ToSqlOutput, ValueRef};
use rusqlite::ToSql;

#[repr(C)]
#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum JobState {
    Queued = 0,
    Done = 1,
    Canceled = 2,
    Doing = 3,
}

impl Display for JobState {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            JobState::Queued => write!(f, "Queued"),
            JobState::Done => write!(f, "Done"),
            JobState::Canceled => write!(f, "Canceled"),
            JobState::Doing => write!(f, "Doing"),
        }
    }
}

impl FromSql for JobState {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value.as_i64()? {
            0 => Ok(JobState::Queued),
            1 => Ok(JobState::Done),
            2 => Ok(JobState::Canceled),
            3 => Ok(JobState::Doing),
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
            JobState::Doing => Ok(ToSqlOutput::Owned(Integer(3))),
        }
    }
}

pub struct Missing;
pub struct Present;

pub struct JobBuilder<StateSet, ScriptSet, RunAtSet> {
    name: Option<String>,
    state: Option<JobState>,
    script: Option<String>,
    run_at: Option<DateTime<Utc>>,
    cwd: Option<PathBuf>,

    _marker: PhantomData<(StateSet, ScriptSet, RunAtSet)>,
}

impl JobBuilder<Missing, Missing, Missing> {
    pub fn new() -> Self {
        Self {
            name: None,
            state: None,
            script: None,
            run_at: None,
            cwd: None,
            _marker: PhantomData,
        }
    }
}

impl<ScriptSet, RunAtSet> JobBuilder<Missing, ScriptSet, RunAtSet> {
    pub fn state(self, state: JobState) -> JobBuilder<Present, ScriptSet, RunAtSet> {
        JobBuilder {
            name: self.name,
            state: Some(state),
            script: self.script,
            run_at: self.run_at,
            cwd: self.cwd,
            _marker: PhantomData,
        }
    }
}

impl<StateSet, RunAtSet> JobBuilder<StateSet, Missing, RunAtSet> {
    pub fn script(self, script: impl Into<String>) -> JobBuilder<StateSet, Present, RunAtSet> {
        JobBuilder {
            name: self.name,
            state: self.state,
            script: Some(script.into()),
            run_at: self.run_at,
            cwd: self.cwd,
            _marker: PhantomData,
        }
    }
}

impl<StateSet, ScriptSet> JobBuilder<StateSet, ScriptSet, Missing> {
    pub fn run_at(self, run_at: DateTime<Utc>) -> JobBuilder<StateSet, ScriptSet, Present> {
        JobBuilder {
            name: self.name,
            state: self.state,
            script: self.script,
            run_at: Some(run_at),
            cwd: self.cwd,
            _marker: PhantomData,
        }
    }
}

impl<StateSet, ScriptSet, RunAtSet> JobBuilder<StateSet, ScriptSet, RunAtSet> {
    pub fn name(self, name: impl Into<String>) -> Self {
        JobBuilder {
            name: Some(name.into()),
            state: self.state,
            script: self.script,
            run_at: self.run_at,
            cwd: self.cwd,
            _marker: PhantomData,
        }
    }

    pub fn cwd(self, cwd: impl Into<PathBuf>) -> Self {
        JobBuilder {
            name: self.name,
            state: self.state,
            script: self.script,
            run_at: self.run_at,
            cwd: Some(cwd.into()),
            _marker: PhantomData,
        }
    }
}

impl JobBuilder<Present, Present, Present> {
    pub fn build(self) -> Job {
        let cwd = env::current_dir().unwrap();

        Job {
            id: 0.into(),
            name: self.name,
            state: self.state.unwrap(),
            script: self.script.unwrap(),
            run_at: self.run_at.unwrap(),
            cwd,
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct JobID(i64);

impl Display for JobID {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "#{}", self.0)
    }
}

impl Deref for JobID {
    type Target = i64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<i64> for JobID {
    fn from(id: i64) -> Self {
        Self(id)
    }
}

impl FromSql for JobID {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        Ok(JobID(value.as_i64()?))
    }
}

impl ToSql for JobID {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        self.0.to_sql()
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct Job {
    pub id: JobID,
    pub name: Option<String>,
    pub state: JobState,
    pub script: String,
    pub run_at: DateTime<Utc>,
    pub cwd: PathBuf,
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
    pub id: i64,
    pub job_id: JobID,
    pub status: Option<i16>,

    pub stdout: String,
    pub stderr: String,
}

impl JobResult {
    pub fn new(job_id: impl Into<JobID>) -> Self {
        let job_id = job_id.into();
        Self {
            id: 0,
            job_id,
            status: None,
            stdout: String::new(),
            stderr: String::new(),
        }
    }
}
