use crate::PgMajorVersion;

pub const MY_PGVERSION: PgMajorVersion = PgMajorVersion::PG14;

pub const XLOG_DBASE_CREATE: u8 = 0x00;
pub const XLOG_DBASE_DROP: u8 = 0x10;

pub const BKPIMAGE_IS_COMPRESSED: u8 = 0x02; /* page image is compressed */
pub const BKPIMAGE_APPLY: u8 = 0x04; /* page image should be restored during replay */
pub const SIZEOF_RELMAPFILE: usize = 512; /* sizeof(RelMapFile) in relmapper.c */

// List of subdirectories inside pgdata.
// Copied from src/bin/initdb/initdb.c
pub const PGDATA_SUBDIRS: [&str; 22] = [
    "pg_xlog",
    "pg_xlog/archive_status",
    "pg_clog",
    "pg_csnlog",
    "pg_notify",
    "pg_serial",
    "pg_snapshots",
    "pg_twophase",
    "pg_multixact",
    "pg_multixact/members",
    "pg_multixact/offsets",
    "base",
    "base/1",
    "pg_replslot",
    "pg_tblspc",
    "pg_stat_tmp",
    "pg_llog",
    "pg_llog/snapshots",
    "pg_llog/mappings",
    "pg_errorinfo",
    "undo",
    "pg_logical"
];

pub fn bkpimg_is_compressed(bimg_info: u8) -> bool {
    (bimg_info & BKPIMAGE_IS_COMPRESSED) != 0
}

// PG_CONTROL_FILE_SIZE is typically 8192 bytes (one page)
// This constant may not be defined in openGauss headers, so we define it here
pub const PG_CONTROL_FILE_SIZE: u32 = 8192;


