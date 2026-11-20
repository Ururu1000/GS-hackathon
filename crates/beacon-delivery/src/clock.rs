//! Injected time source (SPEC §3: "Anna's window, with an injected clock for tests").

use std::fmt::Debug;
use std::sync::Mutex;

use chrono::{DateTime, Duration, Utc};

pub trait Clock: Send + Sync + Debug {
    fn now(&self) -> DateTime<Utc>;
}

/// Wall-clock time.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// A clock tests can pin and move.
#[derive(Debug)]
pub struct FixedClock(Mutex<DateTime<Utc>>);

impl FixedClock {
    pub fn new(at: DateTime<Utc>) -> Self {
        Self(Mutex::new(at))
    }

    pub fn set(&self, at: DateTime<Utc>) {
        *self.0.lock().expect("clock lock") = at;
    }

    pub fn advance(&self, by: Duration) {
        let mut t = self.0.lock().expect("clock lock");
        *t += by;
    }
}

impl Clock for FixedClock {
    fn now(&self) -> DateTime<Utc> {
        *self.0.lock().expect("clock lock")
    }
}
