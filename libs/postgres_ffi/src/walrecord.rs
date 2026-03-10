//! This module houses types used in decoding of openGauss WAL records.
//!
//! openGauss WAL record format differs from PostgreSQL in several ways:
//! 
//! 1. XLogRecord is 32 bytes (vs 24 in PG):
//!    - xl_tot_len: u32, xl_term: u32, xl_xid: u64, xl_prev: u64,
//!    - xl_info: u8, xl_rmid: u8, xl_bucket_id: u16, xl_crc: u32
//!
//! 2. TransactionId is u64 in openGauss (vs u32 in PG)
//!
//! 3. XLogRecordBlockImageHeader is 4 bytes (vs 5 in PG):
//!    - hole_offset: u16, hole_length: u16 (no bimg_info field!)
//!
//! 4. Block ID has special high bits:
//!    - BKID_HAS_BUCKET_OR_SEGPAGE (0x80): bucket/segment page storage
//!    - BKID_HAS_TDE_PAGE (0x40): TDE (Transparent Data Encryption) page
//!    - Actual block_id is in lower 6 bits (0x3F mask)
//!
//! 5. RepOriginId is int (4 bytes) vs uint16 (2 bytes) in PG
//!
//! 6. Each block reference has an additional last_lsn (u64) field

use bytes::{Buf, Bytes};
use postgres_ffi_types::TimestampTz;
use serde::{Deserialize, Serialize};
use utils::bin_ser::DeserializeError;
use utils::lsn::Lsn;

use crate::{
    BLCKSZ, BlockNumber, MultiXactId, MultiXactOffset, MultiXactStatus, Oid, PgMajorVersion,
    RepOriginId, TransactionId, XLOG_SIZE_OF_XLOG_RECORD, XLogRecord, pg_constants,
};

#[repr(C)]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct XlMultiXactCreate {
    pub mid: MultiXactId,
    /* new MultiXact's ID */
    pub moff: MultiXactOffset,
    /* its starting offset in members file */
    pub nmembers: u32,
    /* number of member XIDs */
    pub members: Vec<MultiXactMember>,
}

/// openGauss xl_multixact_create structure:
/// typedef struct xl_multixact_create {
///     MultiXactId mid;        // 8 bytes (TransactionId = uint64)
///     MultiXactOffset moff;   // 8 bytes (uint64)
///     int32 nxids;            // 4 bytes
///     TransactionId xids[FLEXIBLE_ARRAY_MEMBER];  // each 8 bytes
/// } xl_multixact_create;
impl XlMultiXactCreate {
    pub fn decode(buf: &mut Bytes) -> XlMultiXactCreate {
        let mid = buf.get_u64_le();
        let moff = buf.get_u64_le();
        let nmembers = buf.get_i32_le() as u32;
        let mut members = Vec::new();
        for _ in 0..nmembers {
            // In openGauss, xids array stores TransactionId with status encoded in high bits
            // low 60 bits record member xid, high 3 bits record member status
            let packed = buf.get_u64_le();
            let xid = packed & 0x0FFF_FFFF_FFFF_FFFF; // low 60 bits
            let status = ((packed >> 60) & 0x7) as u32; // high 3 bits
            members.push(MultiXactMember { xid, status });
        }
        XlMultiXactCreate {
            mid,
            moff,
            nmembers,
            members,
        }
    }
}

#[repr(C)]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct XlMultiXactTruncate {
    pub oldest_multi_db: Oid,
    /* to-be-truncated range of multixact offsets */
    pub start_trunc_off: MultiXactId,
    /* just for completeness' sake */
    pub end_trunc_off: MultiXactId,

    /* to-be-truncated range of multixact members */
    pub start_trunc_memb: MultiXactOffset,
    pub end_trunc_memb: MultiXactOffset,
}

impl XlMultiXactTruncate {
    pub fn decode(buf: &mut Bytes) -> XlMultiXactTruncate {
        XlMultiXactTruncate {
            oldest_multi_db: buf.get_u32_le(),
            // In openGauss, MultiXactId and MultiXactOffset are uint64
            start_trunc_off: buf.get_u64_le(),
            end_trunc_off: buf.get_u64_le(),
            start_trunc_memb: buf.get_u64_le(),
            end_trunc_memb: buf.get_u64_le(),
        }
    }
}

#[repr(C)]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct XlRelmapUpdate {
    pub dbid: Oid,   /* database ID, or 0 for shared map */
    pub tsid: Oid,   /* database's tablespace, or pg_global */
    pub nbytes: i32, /* size of relmap data */
}

impl XlRelmapUpdate {
    pub fn decode(buf: &mut Bytes) -> XlRelmapUpdate {
        XlRelmapUpdate {
            dbid: buf.get_u32_le(),
            tsid: buf.get_u32_le(),
            nbytes: buf.get_i32_le(),
        }
    }
}

#[repr(C)]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct XlReploriginDrop {
    pub node_id: RepOriginId,
}

impl XlReploriginDrop {
    pub fn decode(buf: &mut Bytes) -> XlReploriginDrop {
        XlReploriginDrop {
            // In openGauss, RepOriginId is int (i32)
            node_id: buf.get_i32_le(),
        }
    }
}

#[repr(C)]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct XlReploriginSet {
    pub remote_lsn: Lsn,
    pub node_id: RepOriginId,
}

impl XlReploriginSet {
    pub fn decode(buf: &mut Bytes) -> XlReploriginSet {
        XlReploriginSet {
            remote_lsn: Lsn(buf.get_u64_le()),
            // In openGauss, RepOriginId is int (i32)
            node_id: buf.get_i32_le(),
        }
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct RelFileNode {
    pub spcnode: Oid, /* tablespace */
    pub dbnode: Oid,  /* database */
    pub relnode: Oid, /* relation */
}

#[repr(C)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MultiXactMember {
    pub xid: TransactionId,
    pub status: MultiXactStatus,
}

/// openGauss MultiXactMember structure:
/// typedef struct MultiXactMember {
///     TransactionId xid;      // 8 bytes (uint64 in openGauss)
///     MultiXactStatus status; // 4 bytes
/// } MultiXactMember;
impl MultiXactMember {
    pub fn decode(buf: &mut Bytes) -> MultiXactMember {
        MultiXactMember {
            xid: buf.get_u64_le(),
            status: buf.get_u32_le(),
        }
    }
}

/// DecodedBkpBlock represents per-page data contained in a WAL record.
#[derive(Default)]
pub struct DecodedBkpBlock {
    /* Is this block ref in use? */
    //in_use: bool,

    /* Identify the block this refers to */
    pub rnode_spcnode: u32,
    pub rnode_dbnode: u32,
    pub rnode_relnode: u32,
    // Note that we have a few special forknum values for non-rel files.
    pub forknum: u8,
    pub blkno: u32,

    /* copy of the fork_flags field from the XLogRecordBlockHeader */
    pub flags: u8,

    /* Information on full-page image, if any */
    pub has_image: bool,
    /* has image, even for consistency checking */
    pub apply_image: bool,
    /* has image that should be restored */
    pub will_init: bool,
    /* record doesn't need previous page version to apply */
    //char	   *bkp_image;
    pub hole_offset: u16,
    pub hole_length: u16,
    pub bimg_offset: u32,
    pub bimg_len: u16,
    pub bimg_info: u8,

    /* Buffer holding the rmgr-specific data associated with this block */
    has_data: bool,
    data_len: u16,
}

impl DecodedBkpBlock {
    pub fn new() -> DecodedBkpBlock {
        Default::default()
    }
}

#[derive(Default)]
pub struct DecodedWALRecord {
    pub xl_xid: TransactionId,
    pub xl_info: u8,
    pub xl_rmid: u8,
    pub record: Bytes, // raw XLogRecord

    pub blocks: Vec<DecodedBkpBlock>,
    pub main_data_offset: usize,
    pub origin_id: u16,
}

impl DecodedWALRecord {
    /// Check if this WAL record represents a legacy "copy" database creation, which populates new relations
    /// by reading other existing relations' data blocks.  This is more complex to apply than new-style database
    /// creations which simply include all the desired blocks in the WAL, so we need a helper function to detect this case.
    pub fn is_dbase_create_copy(&self, pg_version: PgMajorVersion) -> bool {
        if self.xl_rmid == pg_constants::RM_DBASE_ID {
            let info = self.xl_info & pg_constants::XLR_RMGR_INFO_MASK;
            match pg_version {
                PgMajorVersion::PG14 => {
                    // Postgres 14 database creations are always the legacy kind
                    info == crate::V702::bindings::XLOG_DBASE_CREATE
                }
                _ => false,
            }
        } else {
            false
        }
    }
}

/// Main routine to decode a WAL record and figure out which blocks are modified
//
// See xlogrecord.h for details
// The overall layout of an XLOG record is:
//		Fixed-size header (XLogRecord struct)
//      XLogRecordBlockHeader struct
//          If pg_constants::BKPBLOCK_HAS_IMAGE, an XLogRecordBlockImageHeader struct follows
//	           If pg_constants::BKPIMAGE_HAS_HOLE and pg_constants::BKPIMAGE_IS_COMPRESSED, an
//	           XLogRecordBlockCompressHeader struct follows.
//          If pg_constants::BKPBLOCK_SAME_REL is not set, a RelFileNode follows
//          BlockNumber follows
//      XLogRecordBlockHeader struct
//      ...
//      XLogRecordDataHeader[Short|Long] struct
//      block data
//      block data
//      ...
//      main data
//
//
// For performance reasons, the caller provides the DecodedWALRecord struct and the function just fills it in.
// It would be more natural for this function to return a DecodedWALRecord as return value,
// but reusing the caller-supplied struct avoids an allocation.
// This code is in the hot path for digesting incoming WAL, and is very performance sensitive.
//
pub fn decode_wal_record(
    record: Bytes,
    decoded: &mut DecodedWALRecord,
    pg_version: PgMajorVersion,
) -> anyhow::Result<()> {
    let mut rnode_spcnode: u32 = 0;
    let mut rnode_dbnode: u32 = 0;
    let mut rnode_relnode: u32 = 0;
    let mut got_rnode = false;
    let mut origin_id: u16 = 0;

    let mut buf = record.clone();

    // 1. Parse XLogRecord struct

    // FIXME: assume little-endian here
    let xlogrec = XLogRecord::from_bytes(&mut buf)?;

    tracing::info!(
        "TESTDBG decode_wal_record: record_len={}, xl_tot_len={}, xl_rmid={}, xl_info={}, xl_xid={}, xl_prev={}, XLOG_SIZE_OF_XLOG_RECORD={}",
        record.len(),
        xlogrec.xl_tot_len,
        xlogrec.xl_rmid,
        xlogrec.xl_info,
        xlogrec.xl_xid,
        xlogrec.xl_prev,
        XLOG_SIZE_OF_XLOG_RECORD
    );

    let remaining: usize = xlogrec.xl_tot_len as usize - XLOG_SIZE_OF_XLOG_RECORD;

    if buf.remaining() != remaining {
        tracing::error!(
            "TESTDBG decode_wal_record: buf.remaining()={} != remaining={}, xl_tot_len={}",
            buf.remaining(),
            remaining,
            xlogrec.xl_tot_len
        );
        anyhow::bail!(
            "WAL record size mismatch: buf.remaining()={} != expected remaining={} (xl_tot_len={})",
            buf.remaining(),
            remaining,
            xlogrec.xl_tot_len
        );
    }

    let mut max_block_id = 0;
    let mut blocks_total_len: u32 = 0;
    let mut main_data_len = 0;
    let mut datatotal: u32 = 0;
    decoded.blocks.clear();

    // 2. Decode the headers.
    // XLogRecordBlockHeaders if any,
    // XLogRecordDataHeader[Short|Long]
    while buf.remaining() > datatotal as usize {
        let block_id = buf.get_u8();

        match block_id {
            pg_constants::XLR_BLOCK_ID_DATA_SHORT => {
                /* XLogRecordDataHeaderShort */
                main_data_len = buf.get_u8() as u32;
                datatotal += main_data_len;
            }

            pg_constants::XLR_BLOCK_ID_DATA_LONG => {
                /* XLogRecordDataHeaderLong */
                main_data_len = buf.get_u32_le();
                datatotal += main_data_len;
            }

            pg_constants::XLR_BLOCK_ID_ORIGIN => {
                // In openGauss, RepOriginId is int (i32, 4 bytes)
                // In PostgreSQL, RepOriginId is uint16 (2 bytes)
                // We need to read 4 bytes for openGauss
                origin_id = buf.get_i32_le() as u16;
            }

            pg_constants::XLR_BLOCK_ID_TOPLEVEL_XID => {
                // In openGauss, TransactionId is uint64 (8 bytes)
                // In PostgreSQL, TransactionId is uint32 (4 bytes)
                // We need to skip 8 bytes for openGauss
                buf.advance(8);
            }

            _ => {
                // openGauss uses high bits of block_id for special flags:
                // - BKID_HAS_BUCKET_OR_SEGPAGE (0x80): indicates bucket/segment page storage
                // - BKID_HAS_TDE_PAGE (0x40): indicates TDE (Transparent Data Encryption) page
                // The actual block_id is in the lower 6 bits (0x3F mask)
                const BKID_HAS_BUCKET_OR_SEGPAGE: u8 = 0x80;
                const BKID_HAS_TDE_PAGE: u8 = 0x40;
                const BKID_MASK: u8 = 0x3F;
                
                // TdeInfo size in openGauss (from data_common.h):
                // DEK_CIPHER_LEN(320) + CMK_ID_LEN(40) + RANDOM_IV_LEN(16) + GCM_TAG_LEN(16) + algo(1) + res(3) = 396
                const SIZEOF_TDE_INFO: usize = 396;
                
                let actual_block_id = block_id & BKID_MASK;
                let has_bucket_or_segpage = (block_id & BKID_HAS_BUCKET_OR_SEGPAGE) != 0;
                let has_tde_page = (block_id & BKID_HAS_TDE_PAGE) != 0;
                
                tracing::info!(
                    "TESTDBG decode_wal_record: block_id=0x{:02x}, actual_block_id={}, has_bucket_or_segpage={}, has_tde_page={}, buf.remaining()={}",
                    block_id,
                    actual_block_id,
                    has_bucket_or_segpage,
                    has_tde_page,
                    buf.remaining()
                );
                
                // Check if this is a valid block reference
                if actual_block_id > pg_constants::XLR_MAX_BLOCK_ID {
                    // Invalid block_id, skip
                    continue;
                }
                
                /* XLogRecordBlockHeader */
                let mut blk = DecodedBkpBlock::new();

                if actual_block_id <= max_block_id && actual_block_id != 0 {
                    // TODO
                    //report_invalid_record(state,
                    //			  "out-of-order block_id %u at %X/%X",
                    //			  block_id,
                    //			  (uint32) (state->ReadRecPtr >> 32),
                    //			  (uint32) state->ReadRecPtr);
                    //    goto err;
                }
                max_block_id = actual_block_id;

                let fork_flags: u8 = buf.get_u8();
                blk.forknum = fork_flags & pg_constants::BKPBLOCK_FORK_MASK;
                blk.flags = fork_flags;
                blk.has_image = (fork_flags & pg_constants::BKPBLOCK_HAS_IMAGE) != 0;
                blk.has_data = (fork_flags & pg_constants::BKPBLOCK_HAS_DATA) != 0;
                blk.will_init = (fork_flags & pg_constants::BKPBLOCK_WILL_INIT) != 0;
                blk.data_len = buf.get_u16_le();
                
                tracing::info!(
                    "TESTDBG decode_wal_record: fork_flags=0x{:02x}, has_image={}, has_data={}, data_len={}, buf.remaining()={}",
                    fork_flags,
                    blk.has_image,
                    blk.has_data,
                    blk.data_len,
                    buf.remaining()
                );

                /* TODO cross-check that the HAS_DATA flag is set iff data_length > 0 */

                datatotal += blk.data_len as u32;
                blocks_total_len += blk.data_len as u32;

                if blk.has_image {
                    // openGauss XLogRecordBlockImageHeader is different from PostgreSQL:
                    // openGauss: { hole_offset: u16, hole_length: u16 } - 4 bytes total
                    // PostgreSQL: { length: u16, hole_offset: u16, bimg_info: u8 } - 5 bytes
                    //
                    // In openGauss:
                    // - bimg_len = BLCKSZ - hole_length
                    // - No compression support (no bimg_info field)
                    blk.hole_offset = buf.get_u16_le();
                    blk.hole_length = buf.get_u16_le();
                    blk.bimg_len = BLCKSZ - blk.hole_length;
                    blk.bimg_info = 0; // openGauss has no bimg_info field
                    blk.apply_image = true; // Always apply in openGauss

                    tracing::info!(
                        "TESTDBG decode_wal_record: block image header: hole_offset={}, hole_length={}, bimg_len={}",
                        blk.hole_offset,
                        blk.hole_length,
                        blk.bimg_len
                    );

                    datatotal += blk.bimg_len as u32;
                    blocks_total_len += blk.bimg_len as u32;

                    // openGauss doesn't have bimg_info field, so we skip the PostgreSQL-specific
                    // cross-checks that rely on BKPIMAGE_HAS_HOLE and compression flags
                }
                
                // openGauss block header parsing order (from xlogreader.cpp DecodeXLogRecord):
                // 1. fork_flags, data_len (already read above)
                // 2. If has_image: hole_offset, hole_length (already read above)
                // 3. If !BKPBLOCK_SAME_REL:
                //    - RelFileNode (spcnode, dbnode, relnode)
                //    - If BKID_HAS_BUCKET_OR_SEGPAGE: bucketNode, opt
                //    - If BKID_HAS_TDE_PAGE: TdeInfo (396 bytes)
                //    - extra_flag (u16)
                // 4. blkno (u32)
                // 5. If XLOG_NEED_PHYSICAL_LOCATION: seg_fileno, seg_blockno, [vm_seg_fileno, vm_seg_blockno]
                // 6. last_lsn (u64)
                
                if fork_flags & pg_constants::BKPBLOCK_SAME_REL == 0 {
                    // Read RelFileNode
                    rnode_spcnode = buf.get_u32_le();
                    rnode_dbnode = buf.get_u32_le();
                    rnode_relnode = buf.get_u32_le();
                    
                    tracing::info!(
                        "TESTDBG decode_wal_record: RelFileNode: spc={}, db={}, rel={}, buf.remaining()={}",
                        rnode_spcnode, rnode_dbnode, rnode_relnode, buf.remaining()
                    );
                    
                    // openGauss RelFileNode has additional fields when BKID_HAS_BUCKET_OR_SEGPAGE is set:
                    // - bucketNode (int2, 2 bytes)
                    // - opt (uint2, 2 bytes)
                    if has_bucket_or_segpage {
                        let bucket_node = buf.get_i16_le();
                        let opt = buf.get_u16_le();
                        tracing::info!(
                            "TESTDBG decode_wal_record: bucket_or_segpage: bucket_node={}, opt={}, buf.remaining()={}",
                            bucket_node, opt, buf.remaining()
                        );
                    }
                    
                    // If TDE page, skip TdeInfo structure (396 bytes)
                    if has_tde_page {
                        tracing::info!(
                            "TESTDBG decode_wal_record: skipping TdeInfo ({} bytes), buf.remaining()={}",
                            SIZEOF_TDE_INFO, buf.remaining()
                        );
                        buf.advance(SIZEOF_TDE_INFO);
                    }
                    
                    // openGauss has extra_flag (uint16) after RelFileNode/TdeInfo
                    let extra_flag = buf.get_u16_le();
                    tracing::info!(
                        "TESTDBG decode_wal_record: extra_flag=0x{:04x}, buf.remaining()={}",
                        extra_flag, buf.remaining()
                    );
                    
                    got_rnode = true;
                } else if !got_rnode {
                    // BKPBLOCK_SAME_REL is set but no previous rel
                    // In openGauss, extra_flag is copied from previous block, not read from WAL
                    anyhow::bail!(
                        "BKPBLOCK_SAME_REL set but no previous rel in WAL record"
                    );
                } else {
                    tracing::info!(
                        "TESTDBG decode_wal_record: BKPBLOCK_SAME_REL set, using previous rnode: {}/{}/{}",
                        rnode_spcnode, rnode_dbnode, rnode_relnode
                    );
                }
                // When BKPBLOCK_SAME_REL is set, rnode and extra_flag are inherited from lastBlock
                // No additional data to read for RelFileNode

                blk.rnode_spcnode = rnode_spcnode;
                blk.rnode_dbnode = rnode_dbnode;
                blk.rnode_relnode = rnode_relnode;

                // Read block number
                blk.blkno = buf.get_u32_le();
                
                tracing::info!(
                    "TESTDBG decode_wal_record: blkno={}, buf.remaining()={}",
                    blk.blkno, buf.remaining()
                );
                
                // openGauss: if segment-page storage, read physical location
                // For now, we assume we're not using segment storage (XLOG_NEED_PHYSICAL_LOCATION returns false)
                // If using segment storage, we would need to read:
                // - seg_fileno (uint8)
                // - seg_blockno (BlockNumber/uint32)
                // - If seg_fileno & BKPBLOCK_HAS_VM_LOC:
                //   - vm_seg_fileno (uint8)
                //   - vm_seg_blockno (BlockNumber/uint32)
                
                // openGauss has last_lsn (XLogRecPtr, 8 bytes) at the end of each block header
                let last_lsn = buf.get_u64_le();
                
                tracing::info!(
                    "TESTDBG decode_wal_record: last_lsn={:X}/{:X}, affects {}/{}/{} blk {}, buf.remaining()={}",
                    (last_lsn >> 32) as u32, last_lsn as u32,
                    rnode_spcnode,
                    rnode_dbnode,
                    rnode_relnode,
                    blk.blkno,
                    buf.remaining()
                );

                decoded.blocks.push(blk);
            }
        }
    }

    // 3. Decode blocks.
    let mut ptr = record.len() - buf.remaining();
    for blk in decoded.blocks.iter_mut() {
        if blk.has_image {
            blk.bimg_offset = ptr as u32;
            ptr += blk.bimg_len as usize;
        }
        if blk.has_data {
            ptr += blk.data_len as usize;
        }
    }
    // We don't need them, so just skip blocks_total_len bytes
    buf.advance(blocks_total_len as usize);
    assert_eq!(ptr, record.len() - buf.remaining());

    let main_data_offset = (xlogrec.xl_tot_len - main_data_len) as usize;

    tracing::info!(
        "TESTDBG decode_wal_record: main_data_offset={}, main_data_len={}, xl_tot_len={}, record.len()={}, blocks_total_len={}",
        main_data_offset,
        main_data_len,
        xlogrec.xl_tot_len,
        record.len(),
        blocks_total_len
    );

    // 4. Decode main_data
    if main_data_len > 0 {
        assert_eq!(buf.remaining(), main_data_len as usize);
        // TESTDBG: Print the complete main_data in hex before it's used by heap decoders
        tracing::info!(
            "TESTDBG decode_wal_record: main_data_hex (complete, {} bytes)={:02x?}",
            buf.remaining(),
            &buf[..buf.remaining()]
        );
    }

    decoded.xl_xid = xlogrec.xl_xid;
    decoded.xl_info = xlogrec.xl_info;
    decoded.xl_rmid = xlogrec.xl_rmid;
    decoded.record = record;
    decoded.origin_id = origin_id;
    decoded.main_data_offset = main_data_offset;

    Ok(())
}

pub mod V702 {
    use bytes::{Buf, Bytes};

    use crate::{OffsetNumber, TransactionId};

    #[repr(C)]
    #[derive(Debug)]
    pub struct XlHeapInsert {
        pub offnum: OffsetNumber,
        pub flags: u8,
    }

    /// openGauss xl_heap_insert structure:
    /// typedef struct xl_heap_insert {
    ///     OffsetNumber offnum;  // 2 bytes
    ///     uint8 flags;          // 1 byte
    /// } xl_heap_insert;
    impl XlHeapInsert {
        pub fn decode(buf: &mut Bytes) -> XlHeapInsert {
            tracing::info!(
                "TESTDBG XlHeapInsert::decode: buf.remaining()={}, buf_hex={:02x?}",
                buf.remaining(),
                &buf[..std::cmp::min(buf.remaining(), 32)]
            );
            let offnum = buf.get_u16_le();
            let flags = buf.get_u8();
            tracing::info!(
                "TESTDBG XlHeapInsert::decode: offnum={}, flags=0x{:02x}, remaining={}",
                offnum, flags, buf.remaining()
            );
            XlHeapInsert { offnum, flags }
        }
    }

    #[repr(C)]
    #[derive(Debug)]
    pub struct XlHeapMultiInsert {
        pub flags: u8,
        pub _padding: u8,  // isCompressed in openGauss
        pub ntuples: u16,
    }

    /// openGauss xl_heap_multi_insert structure:
    /// typedef struct xl_heap_multi_insert {
    ///     uint8 flags;
    ///     bool isCompressed;    // 1 byte
    ///     uint16 ntuples;
    ///     OffsetNumber offsets[FLEXIBLE_ARRAY_MEMBER];
    /// } xl_heap_multi_insert;
    impl XlHeapMultiInsert {
        pub fn decode(buf: &mut Bytes) -> XlHeapMultiInsert {
            tracing::info!(
                "TESTDBG XlHeapMultiInsert::decode: buf.remaining()={}, buf_hex={:02x?}",
                buf.remaining(),
                &buf[..std::cmp::min(buf.remaining(), 32)]
            );
            let flags = buf.get_u8();
            let _padding = buf.get_u8();
            let ntuples = buf.get_u16_le();
            tracing::info!(
                "TESTDBG XlHeapMultiInsert::decode: flags=0x{:02x}, ntuples={}, remaining={}",
                flags, ntuples, buf.remaining()
            );
            XlHeapMultiInsert { flags, _padding, ntuples }
        }
    }

    #[repr(C)]
    #[derive(Debug)]
    pub struct XlHeapDelete {
        pub offnum: OffsetNumber,
        pub flags: u8,
        pub xmax: TransactionId,
        pub infobits_set: u8,
    }

    /// openGauss xl_heap_delete structure:
    /// typedef struct xl_heap_delete {
    ///     OffsetNumber offnum;  // 2 bytes
    ///     uint8 flags;          // 1 byte
    ///     TransactionId xmax;   // 8 bytes (uint64 in openGauss)
    ///     uint8 infobits_set;   // 1 byte
    /// } xl_heap_delete;
    ///
    /// SizeOfOldHeapDelete = 3 bytes (only offnum + flags)
    /// SizeOfHeapDelete = 12 bytes (with full xmax + infobits_set)
    impl XlHeapDelete {
        pub fn decode(buf: &mut Bytes) -> XlHeapDelete {
            let buf_len = buf.remaining();
            tracing::info!(
                "TESTDBG XlHeapDelete::decode: buf.remaining()={}, buf_hex={:02x?}",
                buf_len,
                &buf[..std::cmp::min(buf_len, 32)]
            );
            
            let offnum = buf.get_u16_le();
            let flags = buf.get_u8();
            
            // Detect format based on remaining buffer size:
            // - 0 bytes: SizeOfOldHeapDelete format (3 bytes total)
            // - 5 bytes: ShortTransactionId format (3 + 4 + 1 = 8 bytes total, not standard)
            // - 9 bytes: Full format (3 + 8 + 1 = 12 bytes total, SizeOfHeapDelete)
            let remaining = buf.remaining();
            let (xmax, infobits_set) = if remaining == 0 {
                tracing::info!("TESTDBG XlHeapDelete::decode: using SizeOfOldHeapDelete format (3 bytes)");
                (0u64, 0u8)
            } else if remaining >= 9 {
                tracing::info!("TESTDBG XlHeapDelete::decode: using SizeOfHeapDelete format (12 bytes)");
                let xmax = buf.get_u64_le();
                let infobits_set = buf.get_u8();
                (xmax, infobits_set)
            } else if remaining >= 5 {
                // Possible ShortTransactionId format (4 bytes xmax + 1 byte infobits)
                tracing::info!("TESTDBG XlHeapDelete::decode: using ShortTransactionId format ({} bytes)", 3 + remaining);
                let xmax = buf.get_u32_le() as u64;
                let infobits_set = buf.get_u8();
                (xmax, infobits_set)
            } else if remaining >= 4 {
                // ShortTransactionId without infobits_set
                tracing::info!("TESTDBG XlHeapDelete::decode: using ShortTransactionId format without infobits");
                let xmax = buf.get_u32_le() as u64;
                (xmax, 0u8)
            } else {
                tracing::warn!("TESTDBG XlHeapDelete::decode: UNEXPECTED remaining={}", remaining);
                (0u64, 0u8)
            };
            
            tracing::info!(
                "TESTDBG XlHeapDelete::decode: offnum={}, flags=0x{:02x}, xmax={}, infobits_set=0x{:02x}, final_remaining={}",
                offnum, flags, xmax, infobits_set, buf.remaining()
            );
            XlHeapDelete { offnum, flags, xmax, infobits_set }
        }
    }

    #[repr(C)]
    #[derive(Debug)]
    pub struct XlHeapUpdate {
        pub old_offnum: OffsetNumber,
        pub new_offnum: OffsetNumber,
        pub flags: u8,
        pub old_xmax: TransactionId,
        pub new_xmax: TransactionId,
        pub old_infobits_set: u8,
    }

    /// openGauss xl_heap_update structure:
    /// IMPORTANT: In openGauss, xl_heap_update uses ShortTransactionId (4 bytes) for xmax fields,
    /// NOT full TransactionId (8 bytes). This is different from xl_heap_delete which uses full xid.
    ///
    /// Actual openGauss structure (from htup.h):
    /// typedef struct xl_heap_update {
    ///     OffsetNumber old_offnum;  // 2 bytes
    ///     OffsetNumber new_offnum;  // 2 bytes
    ///     uint8 flags;              // 1 byte
    ///     TransactionId old_xmax;   // 8 bytes full OR 4 bytes short depending on format
    ///     TransactionId new_xmax;   // 8 bytes full OR 4 bytes short depending on format
    ///     uint8 old_infobits_set;   // 1 byte (only in new format, SizeOfHeapUpdate)
    /// } xl_heap_update;
    ///
    /// SizeOfOldHeapUpdate = 5 bytes (only offnums + flags)
    /// SizeOfHeapUpdate = 22 bytes (with full 8-byte xmax fields)
    /// When using ShortTransactionId: 2+2+1+4+4 = 13 bytes (no infobits_set)
    impl XlHeapUpdate {
        pub fn decode(buf: &mut Bytes) -> XlHeapUpdate {
            let buf_len = buf.remaining();
            tracing::info!(
                "TESTDBG XlHeapUpdate::decode: buf.remaining()={}, buf_hex={:02x?}",
                buf_len,
                &buf[..std::cmp::min(buf_len, 32)]
            );
            
            let old_offnum = buf.get_u16_le();
            let new_offnum = buf.get_u16_le();
            let flags = buf.get_u8();
            
            tracing::info!(
                "TESTDBG XlHeapUpdate::decode: old_offnum={}, new_offnum={}, flags=0x{:02x}, remaining_after_flags={}",
                old_offnum, new_offnum, flags, buf.remaining()
            );
            
            // Determine format based on remaining buffer size:
            // - 0 bytes remaining after flags: SizeOfOldHeapUpdate format (5 bytes total)
            // - 8 bytes remaining: ShortTransactionId format (13 bytes total, no infobits_set)
            // - 17 bytes remaining: Full TransactionId format (22 bytes total)
            let remaining = buf.remaining();
            let (old_xmax, new_xmax, old_infobits_set) = if remaining == 0 {
                // SizeOfOldHeapUpdate format - only offnums and flags
                tracing::info!("TESTDBG XlHeapUpdate::decode: using SizeOfOldHeapUpdate format (5 bytes)");
                (0u64, 0u64, 0u8)
            } else if remaining == 8 {
                // ShortTransactionId format (4 bytes each, no infobits_set)
                tracing::info!("TESTDBG XlHeapUpdate::decode: using ShortTransactionId format (13 bytes)");
                let old_xmax = buf.get_u32_le() as u64;
                let new_xmax = buf.get_u32_le() as u64;
                (old_xmax, new_xmax, 0u8)
            } else if remaining == 9 {
                // ShortTransactionId format with infobits_set (4 bytes each + 1 byte)
                tracing::info!("TESTDBG XlHeapUpdate::decode: using ShortTransactionId format with infobits (14 bytes)");
                let old_xmax = buf.get_u32_le() as u64;
                let new_xmax = buf.get_u32_le() as u64;
                let old_infobits_set = buf.get_u8();
                (old_xmax, new_xmax, old_infobits_set)
            } else if remaining >= 17 {
                // Full TransactionId format (8 bytes each + 1 byte infobits_set)
                tracing::info!("TESTDBG XlHeapUpdate::decode: using full TransactionId format (22 bytes)");
                let old_xmax = buf.get_u64_le();
                let new_xmax = buf.get_u64_le();
                let old_infobits_set = buf.get_u8();
                (old_xmax, new_xmax, old_infobits_set)
            } else {
                tracing::error!(
                    "TESTDBG XlHeapUpdate::decode: UNEXPECTED remaining={}, cannot determine format!",
                    remaining
                );
                // Try to read what we can - assume short format without infobits
                if remaining >= 8 {
                    let old_xmax = buf.get_u32_le() as u64;
                    let new_xmax = buf.get_u32_le() as u64;
                    (old_xmax, new_xmax, 0u8)
                } else {
                    (0u64, 0u64, 0u8)
                }
            };
            
            tracing::info!(
                "TESTDBG XlHeapUpdate::decode: old_xmax={}, new_xmax={}, old_infobits_set=0x{:02x}, final_remaining={}",
                old_xmax, new_xmax, old_infobits_set, buf.remaining()
            );
            
            XlHeapUpdate {
                old_offnum,
                new_offnum,
                flags,
                old_xmax,
                new_xmax,
                old_infobits_set,
            }
        }
    }

    #[repr(C)]
    #[derive(Debug)]
    pub struct XlHeapLock {
        pub locking_xid: TransactionId,
        pub offnum: OffsetNumber,
        pub xid_is_mxact: bool,
        pub shared_lock: bool,
        pub infobits_set: u8,
        pub lock_updated: bool,
    }

    /// openGauss xl_heap_lock structure:
    /// typedef struct xl_heap_lock {
    ///     TransactionId locking_xid;  // 8 bytes
    ///     OffsetNumber offnum;        // 2 bytes
    ///     bool xid_is_mxact;          // 1 byte
    ///     bool shared_lock;           // 1 byte
    ///     uint8 infobits_set;         // 1 byte
    ///     bool lock_updated;          // 1 byte
    /// } xl_heap_lock;
    /// 
    /// SizeOfOldHeapLock = offsetof(shared_lock) + sizeof(bool) = 8 + 2 + 1 + 1 = 12 bytes
    /// SizeOfHeapLock = offsetof(lock_updated) + sizeof(bool) = 8 + 2 + 1 + 1 + 1 + 1 = 14 bytes
    impl XlHeapLock {
        pub fn decode(buf: &mut Bytes) -> XlHeapLock {
            let buf_len = buf.remaining();
            tracing::info!(
                "TESTDBG XlHeapLock::decode: buf.remaining()={}, buf_hex={:02x?}",
                buf_len,
                &buf[..std::cmp::min(buf_len, 32)]
            );
            
            let locking_xid = buf.get_u64_le();
            let offnum = buf.get_u16_le();
            let xid_is_mxact = buf.get_u8() != 0;
            let shared_lock = buf.get_u8() != 0;
            
            // Check if we have the full format (with infobits_set and lock_updated)
            // or the old format (SizeOfOldHeapLock, only up to shared_lock)
            let remaining = buf.remaining();
            let (infobits_set, lock_updated) = if remaining >= 2 {
                (buf.get_u8(), buf.get_u8() != 0)
            } else if remaining == 1 {
                (buf.get_u8(), false)
            } else {
                (0u8, false)
            };
            
            tracing::info!(
                "TESTDBG XlHeapLock::decode: locking_xid={}, offnum={}, xid_is_mxact={}, shared_lock={}, infobits_set=0x{:02x}, lock_updated={}, remaining={}",
                locking_xid, offnum, xid_is_mxact, shared_lock, infobits_set, lock_updated, buf.remaining()
            );
            
            XlHeapLock {
                locking_xid,
                offnum,
                xid_is_mxact,
                shared_lock,
                infobits_set,
                lock_updated,
            }
        }
    }

    #[repr(C)]
    #[derive(Debug)]
    pub struct XlHeapLockUpdated {
        pub xmax: TransactionId,
        pub offnum: OffsetNumber,
        pub infobits_set: u8,
        pub flags: u8,
    }

    /// Note: xl_heap_lock_updated may not exist in openGauss or have different structure
    /// Keeping similar to PostgreSQL for now, but with uint64 xmax
    impl XlHeapLockUpdated {
        pub fn decode(buf: &mut Bytes) -> XlHeapLockUpdated {
            let buf_len = buf.remaining();
            tracing::info!(
                "TESTDBG XlHeapLockUpdated::decode: buf.remaining()={}, buf_hex={:02x?}",
                buf_len,
                &buf[..std::cmp::min(buf_len, 32)]
            );
            
            let xmax = buf.get_u64_le();
            let offnum = buf.get_u16_le();
            let infobits_set = buf.get_u8();
            let flags = buf.get_u8();
            
            tracing::info!(
                "TESTDBG XlHeapLockUpdated::decode: xmax={}, offnum={}, infobits_set=0x{:02x}, flags=0x{:02x}, remaining={}",
                xmax, offnum, infobits_set, flags, buf.remaining()
            );
            
            XlHeapLockUpdated {
                xmax,
                offnum,
                infobits_set,
                flags,
            }
        }
    }

    #[repr(C)]
    #[derive(Debug)]
    pub struct XlParameterChange {
        pub max_connections: i32,
        pub max_worker_processes: i32,
        pub max_wal_senders: i32,
        pub max_prepared_xacts: i32,
        pub max_locks_per_xact: i32,
        pub wal_level: i32,
        pub wal_log_hints: bool,
        pub track_commit_timestamp: bool,
        pub _padding: [u8; 2],
    }

    impl XlParameterChange {
        pub fn decode(buf: &mut Bytes) -> XlParameterChange {
            XlParameterChange {
                max_connections: buf.get_i32_le(),
                max_worker_processes: buf.get_i32_le(),
                max_wal_senders: buf.get_i32_le(),
                max_prepared_xacts: buf.get_i32_le(),
                max_locks_per_xact: buf.get_i32_le(),
                wal_level: buf.get_i32_le(),
                wal_log_hints: buf.get_u8() != 0,
                track_commit_timestamp: buf.get_u8() != 0,
                _padding: [buf.get_u8(), buf.get_u8()],
            }
        }
    }
}

pub mod v15 {
    pub use super::V702::{
        XlHeapDelete, XlHeapInsert, XlHeapLock, XlHeapLockUpdated, XlHeapMultiInsert, XlHeapUpdate,
        XlParameterChange,
    };
}

pub mod v16 {
    pub use super::V702::{
        XlHeapDelete, XlHeapInsert, XlHeapLock, XlHeapLockUpdated, XlHeapMultiInsert, XlHeapUpdate,
        XlParameterChange,
    };

    /* Since PG16, we have the Neon RMGR (RM_NEON_ID) to manage Neon-flavored WAL. */
    /* Note: Neon RMGR is not used in openGauss */
    pub mod rm_neon {
        use bytes::{Buf, Bytes};

        use crate::{OffsetNumber, TransactionId};

        #[repr(C)]
        #[derive(Debug)]
        pub struct XlNeonHeapInsert {
            pub offnum: OffsetNumber,
            pub flags: u8,
        }

        impl XlNeonHeapInsert {
            pub fn decode(buf: &mut Bytes) -> XlNeonHeapInsert {
                XlNeonHeapInsert {
                    offnum: buf.get_u16_le(),
                    flags: buf.get_u8(),
                }
            }
        }

        #[repr(C)]
        #[derive(Debug)]
        pub struct XlNeonHeapMultiInsert {
            pub flags: u8,
            pub _padding: u8,
            pub ntuples: u16,
            pub t_cid: u32,
        }

        impl XlNeonHeapMultiInsert {
            pub fn decode(buf: &mut Bytes) -> XlNeonHeapMultiInsert {
                XlNeonHeapMultiInsert {
                    flags: buf.get_u8(),
                    _padding: buf.get_u8(),
                    ntuples: buf.get_u16_le(),
                    t_cid: buf.get_u32_le(),
                }
            }
        }

        #[repr(C)]
        #[derive(Debug)]
        pub struct XlNeonHeapDelete {
            pub xmax: TransactionId,
            pub offnum: OffsetNumber,
            pub infobits_set: u8,
            pub flags: u8,
            pub t_cid: u32,
        }

        impl XlNeonHeapDelete {
            pub fn decode(buf: &mut Bytes) -> XlNeonHeapDelete {
                XlNeonHeapDelete {
                    xmax: buf.get_u64_le(),
                    offnum: buf.get_u16_le(),
                    infobits_set: buf.get_u8(),
                    flags: buf.get_u8(),
                    t_cid: buf.get_u32_le(),
                }
            }
        }

        #[repr(C)]
        #[derive(Debug)]
        pub struct XlNeonHeapUpdate {
            pub old_xmax: TransactionId,
            pub old_offnum: OffsetNumber,
            pub old_infobits_set: u8,
            pub flags: u8,
            pub t_cid: u32,
            pub new_xmax: TransactionId,
            pub new_offnum: OffsetNumber,
        }

        impl XlNeonHeapUpdate {
            pub fn decode(buf: &mut Bytes) -> XlNeonHeapUpdate {
                XlNeonHeapUpdate {
                    old_xmax: buf.get_u64_le(),
                    old_offnum: buf.get_u16_le(),
                    old_infobits_set: buf.get_u8(),
                    flags: buf.get_u8(),
                    t_cid: buf.get_u32_le(),
                    new_xmax: buf.get_u64_le(),
                    new_offnum: buf.get_u16_le(),
                }
            }
        }

        #[repr(C)]
        #[derive(Debug)]
        pub struct XlNeonHeapLock {
            pub locking_xid: TransactionId,
            pub t_cid: u32,
            pub offnum: OffsetNumber,
            pub infobits_set: u8,
            pub flags: u8,
        }

        impl XlNeonHeapLock {
            pub fn decode(buf: &mut Bytes) -> XlNeonHeapLock {
                XlNeonHeapLock {
                    locking_xid: buf.get_u64_le(),
                    t_cid: buf.get_u32_le(),
                    offnum: buf.get_u16_le(),
                    infobits_set: buf.get_u8(),
                    flags: buf.get_u8(),
                }
            }
        }
    }
}

pub mod v17 {
    use bytes::{Buf, Bytes};

    pub use super::V702::{
        XlHeapDelete, XlHeapInsert, XlHeapLock, XlHeapLockUpdated, XlHeapMultiInsert, XlHeapUpdate,
        XlParameterChange,
    };
    pub use super::v16::rm_neon;
    pub use crate::TimeLineID;
    pub use postgres_ffi_types::TimestampTz;

    #[repr(C)]
    #[derive(Debug)]
    pub struct XlEndOfRecovery {
        pub end_time: TimestampTz,
        pub this_time_line_id: TimeLineID,
        pub prev_time_line_id: TimeLineID,
        pub wal_level: i32,
    }

    impl XlEndOfRecovery {
        pub fn decode(buf: &mut Bytes) -> XlEndOfRecovery {
            XlEndOfRecovery {
                end_time: buf.get_i64_le(),
                this_time_line_id: buf.get_u32_le(),
                prev_time_line_id: buf.get_u32_le(),
                wal_level: buf.get_i32_le(),
            }
        }
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct XlSmgrCreate {
    pub rnode: RelFileNode,
    // FIXME: This is ForkNumber in storage_xlog.h. That's an enum. Does it have
    // well-defined size?
    pub forknum: u8,
}

impl XlSmgrCreate {
    pub fn decode(buf: &mut Bytes) -> XlSmgrCreate {
        XlSmgrCreate {
            rnode: RelFileNode {
                spcnode: buf.get_u32_le(), /* tablespace */
                dbnode: buf.get_u32_le(),  /* database */
                relnode: buf.get_u32_le(), /* relation */
            },
            forknum: buf.get_u32_le() as u8,
        }
    }
}

#[repr(C)]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct XlSmgrTruncate {
    pub blkno: BlockNumber,
    pub rnode: RelFileNode,
    pub flags: u32,
}

impl XlSmgrTruncate {
    pub fn decode(buf: &mut Bytes) -> XlSmgrTruncate {
        XlSmgrTruncate {
            blkno: buf.get_u32_le(),
            rnode: RelFileNode {
                spcnode: buf.get_u32_le(), /* tablespace */
                dbnode: buf.get_u32_le(),  /* database */
                relnode: buf.get_u32_le(), /* relation */
            },
            flags: buf.get_u32_le(),
        }
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct XlCreateDatabase {
    pub db_id: Oid,
    pub tablespace_id: Oid,
    pub src_db_id: Oid,
    pub src_tablespace_id: Oid,
}

impl XlCreateDatabase {
    pub fn decode(buf: &mut Bytes) -> XlCreateDatabase {
        XlCreateDatabase {
            db_id: buf.get_u32_le(),
            tablespace_id: buf.get_u32_le(),
            src_db_id: buf.get_u32_le(),
            src_tablespace_id: buf.get_u32_le(),
        }
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct XlDropDatabase {
    pub db_id: Oid,
    pub n_tablespaces: Oid, /* number of tablespace IDs */
    pub tablespace_ids: Vec<Oid>,
}

impl XlDropDatabase {
    pub fn decode(buf: &mut Bytes) -> XlDropDatabase {
        let mut rec = XlDropDatabase {
            db_id: buf.get_u32_le(),
            n_tablespaces: buf.get_u32_le(),
            tablespace_ids: Vec::<Oid>::new(),
        };

        for _i in 0..rec.n_tablespaces {
            let id = buf.get_u32_le();
            rec.tablespace_ids.push(id);
        }

        rec
    }
}

///
/// Note: Parsing some fields is missing, because they're not needed.
///
/// This is similar to the xl_xact_parsed_commit and
/// xl_xact_parsed_abort structs in PostgreSQL, but we use the same
/// struct for commits and aborts.
///
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct XlXactParsedRecord {
    pub xid: TransactionId,
    pub info: u8,
    pub xact_time: TimestampTz,
    pub xinfo: u32,

    pub db_id: Oid,
    /* MyDatabaseId */
    pub ts_id: Oid,
    /* MyDatabaseTableSpace */
    pub subxacts: Vec<TransactionId>,

    pub xnodes: Vec<RelFileNode>,
    pub origin_lsn: Lsn,
}

impl XlXactParsedRecord {
    /// Decode a XLOG_XACT_COMMIT/ABORT/COMMIT_PREPARED/ABORT_PREPARED
    /// record. This should agree with the ParseCommitRecord and ParseAbortRecord
    /// functions in PostgreSQL (in src/backend/access/rmgr/xactdesc.c)
    /// 
    /// Note: openGauss has different structure than PostgreSQL:
    /// - TransactionId is uint64 (not uint32)
    /// - xl_xact_commit has an extra csn (uint64) field
    /// - xinfo is uint64 (not uint32)
    pub fn decode(buf: &mut Bytes, mut xid: TransactionId, xl_info: u8) -> XlXactParsedRecord {
        let info = xl_info & pg_constants::XLOG_XACT_OPMASK;
        // The record starts with time of commit/abort
        let xact_time = buf.get_i64_le();
        
        // openGauss: skip csn (commit sequence number) - uint64
        let _csn = buf.get_u64_le();
        
        // openGauss: xinfo is uint64 (PostgreSQL uses uint32)
        let xinfo = if xl_info & pg_constants::XLOG_XACT_HAS_INFO != 0 {
            buf.get_u64_le() as u32  // We only use lower 32 bits for flags
        } else {
            0
        };
        let db_id;
        let ts_id;
        if xinfo & pg_constants::XACT_XINFO_HAS_DBINFO != 0 {
            db_id = buf.get_u32_le();
            ts_id = buf.get_u32_le();
        } else {
            db_id = 0;
            ts_id = 0;
        }
        let mut subxacts = Vec::<TransactionId>::new();
        if xinfo & pg_constants::XACT_XINFO_HAS_SUBXACTS != 0 {
            let nsubxacts = buf.get_i32_le();
            for _i in 0..nsubxacts {
                // openGauss: TransactionId is uint64
                let subxact = buf.get_u64_le();
                subxacts.push(subxact);
            }
        }
        let mut xnodes = Vec::<RelFileNode>::new();
        if xinfo & pg_constants::XACT_XINFO_HAS_RELFILENODES != 0 {
            let nrels = buf.get_i32_le();
            for _i in 0..nrels {
                let spcnode = buf.get_u32_le();
                let dbnode = buf.get_u32_le();
                let relnode = buf.get_u32_le();
                tracing::trace!(
                    "XLOG_XACT_COMMIT relfilenode {}/{}/{}",
                    spcnode,
                    dbnode,
                    relnode
                );
                xnodes.push(RelFileNode {
                    spcnode,
                    dbnode,
                    relnode,
                });
            }
        }

        if xinfo & pg_constants::XACT_XINFO_HAS_INVALS != 0 {
            let nmsgs = buf.get_i32_le();
            let sizeof_shared_invalidation_message = 16;
            buf.advance(
                (nmsgs * sizeof_shared_invalidation_message)
                    .try_into()
                    .unwrap(),
            );
        }

        if xinfo & pg_constants::XACT_XINFO_HAS_TWOPHASE != 0 {
            // openGauss: TransactionId is uint64
            xid = buf.get_u64_le();
            tracing::debug!("XLOG_XACT_COMMIT-XACT_XINFO_HAS_TWOPHASE xid {}", xid);
        }

        let origin_lsn = if xinfo & pg_constants::XACT_XINFO_HAS_ORIGIN != 0 {
            Lsn(buf.get_u64_le())
        } else {
            Lsn::INVALID
        };
        XlXactParsedRecord {
            xid,
            info,
            xact_time,
            xinfo,
            db_id,
            ts_id,
            subxacts,
            xnodes,
            origin_lsn,
        }
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct XlClogTruncate {
    pub pageno: u32,
    pub oldest_xid: TransactionId,
    pub oldest_xid_db: Oid,
}

impl XlClogTruncate {
    pub fn decode(buf: &mut Bytes, pg_version: PgMajorVersion) -> XlClogTruncate {
        XlClogTruncate {
            pageno: if pg_version < PgMajorVersion::PG17 {
                buf.get_u32_le()
            } else {
                buf.get_u64_le() as u32
            },
            // openGauss: TransactionId is uint64
            oldest_xid: buf.get_u64_le(),
            oldest_xid_db: buf.get_u32_le(),
        }
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct XlLogicalMessage {
    pub db_id: Oid,
    pub transactional: bool,
    pub prefix_size: usize,
    pub message_size: usize,
}

impl XlLogicalMessage {
    pub fn decode(buf: &mut Bytes) -> XlLogicalMessage {
        XlLogicalMessage {
            db_id: buf.get_u32_le(),
            transactional: buf.get_u32_le() != 0, // 4-bytes alignment
            prefix_size: buf.get_u64_le() as usize,
            message_size: buf.get_u64_le() as usize,
        }
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct XlRunningXacts {
    pub xcnt: u32,
    pub subxcnt: u32,
    pub subxid_overflow: bool,
    pub next_xid: TransactionId,
    pub oldest_running_xid: TransactionId,
    pub latest_completed_xid: TransactionId,
    pub xids: Vec<TransactionId>,
}

impl XlRunningXacts {
    pub fn decode(buf: &mut Bytes) -> XlRunningXacts {
        let xcnt = buf.get_u32_le();
        let subxcnt = buf.get_u32_le();
        let subxid_overflow = buf.get_u32_le() != 0;
        let next_xid = buf.get_u32_le() as u64;
        let oldest_running_xid = buf.get_u32_le() as u64;
        let latest_completed_xid = buf.get_u32_le() as u64;
        let mut xids = Vec::new();
        for _ in 0..(xcnt + subxcnt) {
            xids.push(buf.get_u32_le() as u64);
        }
        XlRunningXacts {
            xcnt,
            subxcnt,
            subxid_overflow,
            next_xid,
            oldest_running_xid,
            latest_completed_xid,
            xids,
        }
    }
}

pub fn describe_postgres_wal_record(record: &Bytes) -> Result<String, DeserializeError> {
    // TODO: It would be nice to use the PostgreSQL rmgrdesc infrastructure for this.
    // Maybe use the postgres wal redo process, the same used for replaying WAL records?
    // Or could we compile the rmgrdesc routines into the dump_layer_file() binary directly,
    // without worrying about security?
    //
    // But for now, we have a hand-written code for a few common WAL record types here.

    let mut buf = record.clone();

    // 1. Parse XLogRecord struct

    // FIXME: assume little-endian here
    let xlogrec = XLogRecord::from_bytes(&mut buf)?;

    let unknown_str: String;

    let result: &str = match xlogrec.xl_rmid {
        pg_constants::RM_HEAP2_ID => {
            let info = xlogrec.xl_info & pg_constants::XLOG_HEAP_OPMASK;
            match info {
                pg_constants::XLOG_HEAP2_MULTI_INSERT => "HEAP2 MULTI_INSERT",
                pg_constants::XLOG_HEAP2_VISIBLE => "HEAP2 VISIBLE",
                _ => {
                    unknown_str = format!("HEAP2 UNKNOWN_0x{info:02x}");
                    &unknown_str
                }
            }
        }
        pg_constants::RM_HEAP_ID => {
            let info = xlogrec.xl_info & pg_constants::XLOG_HEAP_OPMASK;
            match info {
                pg_constants::XLOG_HEAP_INSERT => "HEAP INSERT",
                pg_constants::XLOG_HEAP_DELETE => "HEAP DELETE",
                pg_constants::XLOG_HEAP_UPDATE => "HEAP UPDATE",
                pg_constants::XLOG_HEAP_HOT_UPDATE => "HEAP HOT_UPDATE",
                _ => {
                    unknown_str = format!("HEAP2 UNKNOWN_0x{info:02x}");
                    &unknown_str
                }
            }
        }
        pg_constants::RM_XLOG_ID => {
            let info = xlogrec.xl_info & pg_constants::XLR_RMGR_INFO_MASK;
            match info {
                pg_constants::XLOG_FPI => "XLOG FPI",
                pg_constants::XLOG_FPI_FOR_HINT => "XLOG FPI_FOR_HINT",
                _ => {
                    unknown_str = format!("XLOG UNKNOWN_0x{info:02x}");
                    &unknown_str
                }
            }
        }
        rmid => {
            let info = xlogrec.xl_info & pg_constants::XLR_RMGR_INFO_MASK;

            unknown_str = format!("UNKNOWN_RM_{rmid} INFO_0x{info:02x}");
            &unknown_str
        }
    };

    Ok(String::from(result))
}
