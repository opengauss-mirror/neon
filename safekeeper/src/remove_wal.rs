use utils::id::NodeId;
use utils::lsn::Lsn;

use crate::timeline_manager::StateSnapshot;

/// Get oldest LSN we still need to keep.
///
/// We hold WAL till it is consumed by
/// 1) pageserver (remote_consistent_lsn)
/// 2) s3 offloading.
/// 3) oggit logical decoding worker, if registered.
/// 4) Additionally we must store WAL since last local commit_lsn because
///    that's where we start looking for last WAL record on start.
///
/// If some peer safekeeper misses data it will fetch it from the remote
/// storage. While it is safe to use inmem values for determining horizon, we
/// use persistent to make possible normal states less surprising. All segments
/// covering LSNs before horizon_lsn can be removed.
pub(crate) fn calc_horizon_lsn(state: &StateSnapshot, extra_horizon_lsn: Option<Lsn>) -> Lsn {
    use std::cmp::min;

    let mut horizon_lsn = state.cfile_remote_consistent_lsn;
    // we don't want to remove WAL that is not yet offloaded to s3
    horizon_lsn = min(horizon_lsn, state.cfile_backup_lsn);
    // oggit_required_lsn mirrors logical slot restart_lsn semantics. When no
    // oggit worker is registered it is Lsn::MAX and does not affect the min.
    horizon_lsn = min(horizon_lsn, state.cfile_oggit_required_lsn);
    // Min by local commit_lsn to be able to begin reading WAL from somewhere on
    // sk start. Technically we don't allow local commit_lsn to be higher than
    // flush_lsn, but let's be double safe by including it as well.
    horizon_lsn = min(horizon_lsn, state.cfile_commit_lsn);
    horizon_lsn = min(horizon_lsn, state.flush_lsn);
    if let Some(extra_horizon_lsn) = extra_horizon_lsn {
        horizon_lsn = min(horizon_lsn, extra_horizon_lsn);
    }

    horizon_lsn
}

/// Calculate a remote WAL deletion horizon that is safe for the whole
/// Safekeeper membership. A missing or stale member deliberately blocks GC.
pub(crate) fn remote_wal_gc_horizon_lsn(
    state: &StateSnapshot,
    extra_horizon_lsn: Option<Lsn>,
    my_id: NodeId,
) -> Option<Lsn> {
    if state.membership.new_members.is_some()
        || state.membership.generation.into_inner() == 0
        || state.membership.members.m.is_empty()
    {
        return None;
    }

    let expected_generation = state.membership.generation.into_inner();
    let mut horizon_lsn = calc_horizon_lsn(state, extra_horizon_lsn);
    horizon_lsn = horizon_lsn.min(state.cfile_peer_horizon_lsn);
    for member in &state.membership.members.m {
        if member.id == my_id {
            continue;
        }

        let peer = state.peers.iter().find(|p| p.sk_id == member.id)?;
        if peer.membership_generation != expected_generation || peer.membership_is_joint {
            return None;
        }
        let peer_horizon = peer
            .cfile_commit_lsn
            .min(peer.cfile_backup_lsn)
            .min(peer.cfile_remote_consistent_lsn)
            .min(peer.cfile_peer_horizon_lsn)
            .min(peer.cfile_oggit_required_lsn);
        horizon_lsn = horizon_lsn.min(peer_horizon);
    }

    (horizon_lsn != Lsn::INVALID).then_some(horizon_lsn)
}

/// Return whether every configured member has durably acknowledged a GC
/// watermark. The local value is read from the control-file snapshot.
pub(crate) fn all_members_acknowledged_gc(
    state: &StateSnapshot,
    gc_lsn: Lsn,
    my_id: NodeId,
) -> bool {
    if state.membership.new_members.is_some()
        || state.membership.generation.into_inner() == 0
        || state.membership.members.m.is_empty()
    {
        return false;
    }

    let expected_generation = state.membership.generation.into_inner();
    for member in &state.membership.members.m {
        if member.id == my_id {
            if state.cfile_remote_wal_gc_lsn < gc_lsn {
                return false;
            }
            continue;
        }

        let Some(peer) = state.peers.iter().find(|p| p.sk_id == member.id) else {
            return false;
        };
        if peer.membership_generation != expected_generation
            || peer.membership_is_joint
            || peer.remote_wal_gc_lsn < gc_lsn
        {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use safekeeper_api::membership::{Configuration, MemberSet, SafekeeperId};
    use safekeeper_api::models::PeerInfo;

    fn member(id: u64) -> SafekeeperId {
        SafekeeperId {
            id: NodeId(id),
            host: format!("sk-{id}"),
            pg_port: 5432,
        }
    }

    fn peer(id: u64, horizon: u64, gc_lsn: u64) -> PeerInfo {
        PeerInfo {
            sk_id: NodeId(id),
            term: 0,
            last_log_term: 0,
            flush_lsn: Lsn(1000),
            commit_lsn: Lsn(1000),
            backup_lsn: Lsn(1000),
            remote_consistent_lsn: Lsn(1000),
            peer_horizon_lsn: Lsn(horizon),
            remote_wal_gc_lsn: Lsn(gc_lsn),
            membership_generation: 1,
            membership_is_joint: false,
            cfile_commit_lsn: Lsn(1000),
            cfile_backup_lsn: Lsn(1000),
            cfile_remote_consistent_lsn: Lsn(1000),
            cfile_peer_horizon_lsn: Lsn(horizon),
            cfile_oggit_required_lsn: Lsn::MAX,
            local_start_lsn: Lsn(1),
            ts: tokio::time::Instant::now(),
            pg_connstr: String::new(),
            http_connstr: String::new(),
            https_connstr: None,
        }
    }

    fn snapshot(members: Vec<u64>, peers: Vec<PeerInfo>) -> StateSnapshot {
        StateSnapshot {
            commit_lsn: Lsn(1000),
            backup_lsn: Lsn(1000),
            remote_consistent_lsn: Lsn(1000),
            cfile_commit_lsn: Lsn(1000),
            cfile_remote_consistent_lsn: Lsn(1000),
            cfile_backup_lsn: Lsn(1000),
            cfile_peer_horizon_lsn: Lsn(1000),
            cfile_remote_wal_gc_lsn: Lsn::INVALID,
            cfile_oggit_required_lsn: Lsn::MAX,
            flush_lsn: Lsn(1000),
            last_log_term: 0,
            cfile_last_persist_at: std::time::Instant::now(),
            inmem_flush_pending: false,
            wal_removal_on_hold: false,
            peers,
            membership: Configuration::new(
                MemberSet::new(members.into_iter().map(member).collect()).unwrap(),
            ),
        }
    }

    #[test]
    fn remote_gc_requires_all_members_and_uses_the_slowest_horizon() {
        let state = snapshot(vec![1, 2], vec![peer(2, 700, 0)]);
        assert_eq!(
            remote_wal_gc_horizon_lsn(&state, None, NodeId(1)),
            Some(Lsn(700))
        );

        let state = snapshot(vec![1, 2], Vec::new());
        assert_eq!(remote_wal_gc_horizon_lsn(&state, None, NodeId(1)), None);
    }

    #[test]
    fn remote_gc_requires_all_members_to_acknowledge() {
        let mut state = snapshot(vec![1, 2], vec![peer(2, 700, 700)]);
        state.cfile_remote_wal_gc_lsn = Lsn(700);
        assert!(all_members_acknowledged_gc(&state, Lsn(700), NodeId(1)));

        state.peers[0].remote_wal_gc_lsn = Lsn(699);
        assert!(!all_members_acknowledged_gc(&state, Lsn(700), NodeId(1)));

        state.membership.new_members = Some(MemberSet::new(vec![member(1), member(2)]).unwrap());
        assert!(!all_members_acknowledged_gc(&state, Lsn(700), NodeId(1)));
    }
}
