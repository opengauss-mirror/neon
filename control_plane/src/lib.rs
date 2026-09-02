//! Local control plane.
//!
//! Can start, configure and stop postgres instances running as a local processes.
//!
//! Intended to be used in integration tests and in CLI tools for
//! local installations.
#![deny(clippy::undocumented_unsafe_blocks)]

mod background_process;
pub mod branch_mappings;
pub mod broker;
pub mod endpoint;
pub mod endpoint_storage;
pub mod local_env;
#[allow(dead_code)]
pub mod merge_oggit {
    include!("bin/merge_oggit.rs");
}
pub mod ops;
pub mod pageserver;
pub mod postgresql_conf;
pub mod safekeeper;
pub mod storage_controller;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BranchMergeStrategy {
    Fail,
    Ours,
    Theirs,
    Manual,
}

impl BranchMergeStrategy {
    pub fn as_str(self) -> &'static str {
        match self {
            BranchMergeStrategy::Fail => "fail",
            BranchMergeStrategy::Ours => "ours",
            BranchMergeStrategy::Theirs => "theirs",
            BranchMergeStrategy::Manual => "manual",
        }
    }
}

impl std::str::FromStr for BranchMergeStrategy {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "fail" => Ok(Self::Fail),
            "ours" => Ok(Self::Ours),
            "theirs" => Ok(Self::Theirs),
            "manual" => Ok(Self::Manual),
            _ => anyhow::bail!("invalid merge strategy {value}"),
        }
    }
}
