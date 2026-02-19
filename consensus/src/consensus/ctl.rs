use super::{Consensus, factory::MultiConsensusManagementStore};
use kaspa_consensusmanager::ConsensusCtl;
use kaspa_database::prelude::DB;
use parking_lot::RwLock;
use std::{
    path::PathBuf,
    sync::{Arc, Weak},
    thread::JoinHandle,
};

pub struct Ctl {
    management_store: Arc<RwLock<MultiConsensusManagementStore>>,
    _consensus_db_ref: Weak<DB>,
    _consensus_db_path: PathBuf,
    consensus: Arc<Consensus>,
    consensus_role: ManagedConsensusRole,
}

#[derive(Clone, Copy)]
pub enum ManagedConsensusRole {
    Active,
    Usurper,
    Staging,
}

impl Ctl {
    pub fn new(
        management_store: Arc<RwLock<MultiConsensusManagementStore>>,
        consensus_db: Arc<DB>,
        consensus: Arc<Consensus>,
        consensus_role: ManagedConsensusRole,
    ) -> Self {
        let _consensus_db_path = consensus_db.path().to_owned();
        let _consensus_db_ref = Arc::downgrade(&consensus_db);
        Self { management_store, _consensus_db_ref, _consensus_db_path, consensus, consensus_role }
    }
}

impl ConsensusCtl for Ctl {
    fn start(&self) -> Vec<JoinHandle<()>> {
        self.consensus.run_processors()
    }

    fn stop(&self) {
        self.consensus.signal_exit()
    }

    fn make_active(&self) {
        let mut store = self.management_store.write();
        match self.consensus_role {
            ManagedConsensusRole::Active => panic!("make_active called on active consensus controller"),
            ManagedConsensusRole::Usurper => panic!("usurper consensus cannot be committed directly; promote it to staging first"),
            ManagedConsensusRole::Staging => store.commit_staging_consensus().unwrap(),
        }
    }
}

/// Impl for test purposes
impl ConsensusCtl for Consensus {
    fn start(&self) -> Vec<JoinHandle<()>> {
        self.run_processors()
    }

    fn stop(&self) {
        self.signal_exit()
    }

    fn make_active(&self) {
        unimplemented!()
    }
}


