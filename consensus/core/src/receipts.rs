use std::{collections::HashMap, sync::Arc};

use kaspa_merkle::{verify_merkle_witness, MerkleWitness};

use crate::header::Header;
use kaspa_hashes::Hash;
#[derive(Clone)]
pub enum Pochm {
    LogPath(LogPathPochm),
    Legacy(LegacyPochm),
}
#[derive(Clone)]
pub struct LegacyPochm {
    pub bfs_map: HashMap<Hash, Arc<Header>>,
    pub top: Hash,
    pub bottom: Hash,
}
impl LegacyPochm {
    pub fn new(bfs_vec: Vec<(Hash, Arc<Header>)>) -> Self {
        let top = bfs_vec.last().unwrap().0;
        let bottom = bfs_vec.first().unwrap().0;
        let mut bfs_map = HashMap::new();
        for (key, val) in bfs_vec.into_iter() {
            bfs_map.insert(key, val);
        }
        Self { bfs_map, top, bottom }
    }
    pub fn verify_path(&self, chain_purporter: Hash) -> bool {
        //verify top consistency and availability
        if self.bfs_map.get(&self.top).is_none_or(|hdr| hdr.hash != self.top) {
            return false;
        }
        let mut next_chain_blk = self.top;
        loop {
            if next_chain_blk == chain_purporter {
                return true;
            }
            //verify parents consistency and availability
            for &par in self.bfs_map.get(&next_chain_blk).unwrap().parents_by_level[0].iter() {
                if self.bfs_map.get(&par).is_none_or(|hdr| hdr.hash != par) {
                    return false;
                }
            }
            next_chain_blk = *self.bfs_map.get(&next_chain_blk).unwrap().parents_by_level[0]
                .iter()
                .map(|blk| (blk, self.bfs_map.get(blk).unwrap().blue_score))
                .reduce(|(blk, bscore), (max_blk, max_bscore)| if bscore > max_bscore { (blk, bscore) } else { (max_blk, max_bscore) })
                .unwrap()
                .0;
        }
    }
}
#[derive(Clone)]
pub struct PochmSegment {
    pub header: Arc<Header>,
    pub leaf_in_pchmr_witness: MerkleWitness,
}

#[derive(Clone)]
pub struct LogPathPochm {
    pub vec: Vec<PochmSegment>,
    // hash_to_pchmr_store: Arc<DbPchmrStore>, //temporary field
}
impl LogPathPochm {
    pub fn new() -> Self {
        let vec = vec![];
        Self { vec }
    }
    pub fn insert(&mut self, header: Arc<Header>, witness: MerkleWitness) {
        self.vec.push(PochmSegment { header, leaf_in_pchmr_witness: witness })
    }
    pub fn get_path_origin(&self) -> Option<Hash> {
        self.vec.first().map(|seg| seg.header.hash)
    }

    pub fn verify_path(&self, destination_block_hash: Hash) -> bool {
        let leaf_hashes = self.vec.iter()
            .skip(1)//remove first element to match accordingly to witnesses 
            .map(|pochm_seg| pochm_seg.header.hash)//map to hashes
            .chain(std::iter::once(destination_block_hash)); // add final block

        /*verify the path from posterity down to chain_purporter:
        iterate downward from posterity block header: for each, verify that leaf hash is in pchmr of ther header */
        self.vec.iter().zip(leaf_hashes).all(|(pochm_seg, leaf_hash)| {
            let pchmr_root_hash = pochm_seg.header.pochm_merkle_root;
            let witness = &pochm_seg.leaf_in_pchmr_witness;
            verify_merkle_witness(witness, leaf_hash, pchmr_root_hash)
        })
    }
}

impl Default for LogPathPochm {
    fn default() -> Self {
        Self::new()
    }
}
#[derive(Clone)]
pub struct TxReceipt {
    pub tracked_tx_id: Hash,
    pub accepting_block_header: Arc<Header>,
    pub pochm: Pochm,
    pub tx_acc_proof: MerkleWitness,
}
#[derive(Clone)]

pub struct ProofOfPublication {
    pub tracked_tx_hash: Hash,
    pub pub_block_header: Arc<Header>,
    pub pochm: Pochm,
    pub tx_pub_proof: MerkleWitness,
    pub headers_path_to_selected: Vec<Arc<Header>>,
}
