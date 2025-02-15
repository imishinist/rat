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
    Dequeued = 1,
    Running = 2,
    Done = 3,
    Canceled = 4,
}

impl Display for JobState {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            JobState::Queued => write!(f, "Queued"),
            JobState::Dequeued => write!(f, "Dequeued"),
            JobState::Running => write!(f, "Running"),
            JobState::Done => write!(f, "Done"),
            JobState::Canceled => write!(f, "Canceled"),
        }
    }
}

impl FromSql for JobState {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value.as_i64()? {
            0 => Ok(JobState::Queued),
            1 => Ok(JobState::Dequeued),
            2 => Ok(JobState::Running),
            3 => Ok(JobState::Done),
            4 => Ok(JobState::Canceled),
            _ => Err(rusqlite::types::FromSqlError::InvalidType),
        }
    }
}

impl ToSql for JobState {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        use rusqlite::types::Value::Integer;
        let state = *self as i64;
        Ok(ToSqlOutput::Owned(Integer(state)))
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
pub struct ID(i64);

impl Display for ID {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "#{}", self.0)
    }
}

impl Deref for ID {
    type Target = i64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<i64> for ID {
    fn from(id: i64) -> Self {
        Self(id)
    }
}

impl FromSql for ID {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        Ok(ID(value.as_i64()?))
    }
}

impl ToSql for ID {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        self.0.to_sql()
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct Job {
    pub id: ID,
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
    pub id: ID,
    pub job_id: ID,
    pub status: Option<i16>,

    pub stdout: String,
    pub stderr: String,
}

impl JobResult {
    pub fn new(job_id: impl Into<ID>) -> Self {
        let job_id = job_id.into();
        Self {
            id: 0.into(),
            job_id,
            status: None,
            stdout: String::new(),
            stderr: String::new(),
        }
    }
}
