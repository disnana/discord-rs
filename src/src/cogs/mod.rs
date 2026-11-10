mod general;

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Default, Serialize, Deserialize)]
pub struct State {
    pub counts: BTreeMap<u64, u64>,
    #[serde(default)]
    pub jobs: BTreeMap<u64, Job>,
}

#[derive(Serialize, Deserialize)]
pub struct Job {
    pub step: u64,
    pub total: u64,
    pub running: bool,
}

pub fn tick(state: &mut State) {
    for job in state.jobs.values_mut() {
        if job.running {
            job.step = job.step.saturating_add(1);
            job.total = job.total.saturating_add(job.step);
        }
    }
}

pub fn dispatch(key: u64, content: &str, state: &mut State) -> Option<String> {
    general::dispatch(key, content, state)
}

pub fn build_id() -> &'static str {
    general::BUILD_ID
}
