//!
//! Basic WAL stream decoding for openGauss.
//!
//! This understands the WAL page and record format, enough to figure out where the WAL record
//! boundaries are, and to reassemble WAL records that cross page boundaries.
//!
//! openGauss XLog format differs from PostgreSQL in several ways:
//! 1. XLogRecord is 32 bytes (vs 24 in PG): adds xl_term (4 bytes) and xl_bucket_id (2 bytes)
//! 2. XLogPageHeaderData is 24 bytes (vs 20 in PG): adds xlp_total_len (4 bytes)
//! 3. XLogLongPageHeaderData is 40 bytes (vs 32+8 in PG)
//! 4. TransactionId is uint64 (vs uint32 in PG)
//! 5. XLOG_PAGE_MAGIC is 0xD074 (vs 0xD10D in PG15)
//! 6. XLogRecordBlockImageHeader is 4 bytes without bimg_info field
//! 7. All records are 8-byte aligned (MAXALIGN)
//!
//! This functionality is needed by both the pageserver and the safekeepers. The pageserver needs
//! to look deeper into the WAL records to also understand which blocks they modify, the code
//! for that is in pageserver/src/walrecord.rs
//!
use super::super::waldecoder::{State, WalDecodeError, WalStreamDecoder};
use super::bindings::{XLogLongPageHeaderData, XLogPageHeaderData, XLogRecord, XLOG_PAGE_MAGIC};
use super::xlog_utils::*;
use crate::WAL_SEGMENT_SIZE;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use crc32c::*;
use log::*;
use std::cmp::min;
use std::num::NonZeroU32;
use utils::lsn::Lsn;

pub trait WalStreamDecoderHandler {
    fn validate_page_header(&self, hdr: &XLogPageHeaderData) -> Result<(), WalDecodeError>;
    fn poll_decode_internal(&mut self) -> Result<Option<(Lsn, Bytes)>, WalDecodeError>;
    fn complete_record(&mut self, recordbuf: Bytes) -> Result<(Lsn, Bytes), WalDecodeError>;
}

//
// This is a trick to support several postgres versions simultaneously.
//
// Page decoding code depends on postgres bindings, so it is compiled for each version.
// Thus WalStreamDecoder implements several WalStreamDecoderHandler traits.
// WalStreamDecoder poll_decode() method dispatches to the right handler based on the postgres version.
// Other methods are internal and are not dispatched.
//
// It is similar to having several impl blocks for the same struct,
// but the impls here are in different modules, so need to use a trait.
//
impl WalStreamDecoderHandler for WalStreamDecoder {
    fn validate_page_header(&self, hdr: &XLogPageHeaderData) -> Result<(), WalDecodeError> {
        let validate_impl = || {
            tracing::info!(
                "TESTDBG validate_page_header: xlp_magic={} (expected {}), xlp_pageaddr={} (expected {}), xlp_info={}, xlp_tli={}, xlp_rem_len={}, xlp_total_len={}",
                hdr.xlp_magic,
                XLOG_PAGE_MAGIC,
                hdr.xlp_pageaddr,
                self.lsn.0,
                hdr.xlp_info,
                hdr.xlp_tli,
                hdr.xlp_rem_len,
                hdr.xlp_total_len
            );
            if hdr.xlp_magic != XLOG_PAGE_MAGIC as u16 {
                return Err(format!(
                    "invalid xlog page header: xlp_magic={}, expected {}",
                    hdr.xlp_magic, XLOG_PAGE_MAGIC
                ));
            }
            if hdr.xlp_pageaddr != self.lsn.0 {
                return Err(format!(
                    "invalid xlog page header: xlp_pageaddr={}, expected {}",
                    hdr.xlp_pageaddr, self.lsn
                ));
            }
            match self.state {
                State::WaitingForRecord => {
                    if hdr.xlp_info & XLP_FIRST_IS_CONTRECORD != 0 {
                        return Err(
                            "invalid xlog page header: unexpected XLP_FIRST_IS_CONTRECORD".into(),
                        );
                    }
                    if hdr.xlp_rem_len != 0 {
                        return Err(format!(
                            "invalid xlog page header: xlp_rem_len={}, but it's not a contrecord",
                            hdr.xlp_rem_len
                        ));
                    }
                }
                State::ReassemblingRecord { contlen, .. } => {
                    if hdr.xlp_info & XLP_FIRST_IS_CONTRECORD == 0 {
                        return Err(
                            "invalid xlog page header: XLP_FIRST_IS_CONTRECORD expected, not found"
                                .into(),
                        );
                    }
                    if hdr.xlp_rem_len != contlen.get() {
                        return Err(format!(
                            "invalid xlog page header: xlp_rem_len={}, expected {}",
                            hdr.xlp_rem_len,
                            contlen.get()
                        ));
                    }
                }
                State::SkippingEverything { .. } => {
                    panic!("Should not be validating page header in the SkippingEverything state");
                }
            };
            Ok(())
        };
        validate_impl().map_err(|msg| WalDecodeError { msg, lsn: self.lsn })
    }

    /// Attempt to decode another WAL record from the input that has been fed to the
    /// decoder so far.
    ///
    /// Returns one of the following:
    ///     Ok((Lsn, Bytes)): a tuple containing the LSN of next record, and the record itself
    ///     Ok(None): there is not enough data in the input buffer. Feed more by calling the `feed_bytes` function
    ///     Err(WalDecodeError): an error occurred while decoding, meaning the input was invalid.
    ///
    fn poll_decode_internal(&mut self) -> Result<Option<(Lsn, Bytes)>, WalDecodeError> {
        // Run state machine that validates page headers, and reassembles records
        // that cross page boundaries.
        loop {
            // parse and verify page boundaries as we go
            // However, we may have to skip some page headers if we're processing the XLOG_SWITCH record or skipping padding for whatever reason.
            match self.state {
                State::WaitingForRecord | State::ReassemblingRecord { .. } => {
                    if self.lsn.segment_offset(WAL_SEGMENT_SIZE) == 0 {
                        // parse long header

                        if self.inputbuf.remaining() < XLOG_SIZE_OF_XLOG_LONG_PHD {
                            return Ok(None);
                        }

                        let hdr = XLogLongPageHeaderData::from_bytes(&mut self.inputbuf).map_err(
                            |e| WalDecodeError {
                                msg: format!("long header deserialization failed {e}"),
                                lsn: self.lsn,
                            },
                        )?;

                        self.validate_page_header(&hdr.std)?;

                        self.lsn += XLOG_SIZE_OF_XLOG_LONG_PHD as u64;
                    } else if self.lsn.block_offset() == 0 {
                        if self.inputbuf.remaining() < XLOG_SIZE_OF_XLOG_SHORT_PHD {
                            return Ok(None);
                        }

                        let hdr =
                            XLogPageHeaderData::from_bytes(&mut self.inputbuf).map_err(|e| {
                                WalDecodeError {
                                    msg: format!("header deserialization failed {e}"),
                                    lsn: self.lsn,
                                }
                            })?;

                        self.validate_page_header(&hdr)?;

                        self.lsn += XLOG_SIZE_OF_XLOG_SHORT_PHD as u64;
                    }
                }
                State::SkippingEverything { .. } => {}
            }
            // now read page contents
            match &mut self.state {
                State::WaitingForRecord => {
                    // need to have at least the xl_tot_len field
                    if self.inputbuf.remaining() < 4 {
                        return Ok(None);
                    }

                    // peek xl_tot_len at the beginning of the record.
                    // FIXME: assumes little-endian
                    let xl_tot_len = (&self.inputbuf[0..4]).get_u32_le();
                    tracing::info!(
                        "TESTDBG poll_decode_internal WaitingForRecord: lsn={}, xl_tot_len={}, inputbuf.remaining()={}, XLOG_SIZE_OF_XLOG_RECORD={}",
                        self.lsn,
                        xl_tot_len,
                        self.inputbuf.remaining(),
                        XLOG_SIZE_OF_XLOG_RECORD
                    );
                    if (xl_tot_len as usize) < XLOG_SIZE_OF_XLOG_RECORD {
                        return Err(WalDecodeError {
                            msg: format!("invalid xl_tot_len {xl_tot_len}"),
                            lsn: self.lsn,
                        });
                    }
                    // Fast path for the common case that the whole record fits on the page.
                    let pageleft = self.lsn.remaining_in_block() as u32;
                    if self.inputbuf.remaining() >= xl_tot_len as usize && xl_tot_len <= pageleft {
                        self.lsn += xl_tot_len as u64;
                        let recordbuf = self.inputbuf.copy_to_bytes(xl_tot_len as usize);
                        return Ok(Some(self.complete_record(recordbuf)?));
                    } else {
                        tracing::info!(
                            "TESTDBG poll_decode_internal: need to reassemble record, xl_tot_len={}, pageleft={}",
                            xl_tot_len,
                            pageleft
                        );
                        // Need to assemble the record from pieces. Remember the size of the
                        // record, and loop back. On next iterations, we will reach the branch
                        // below, and copy the part of the record that was on this or next page(s)
                        // to 'recordbuf'.  Subsequent iterations will skip page headers, and
                        // append the continuations from the next pages to 'recordbuf'.
                        self.state = State::ReassemblingRecord {
                            recordbuf: BytesMut::with_capacity(xl_tot_len as usize),
                            contlen: NonZeroU32::new(xl_tot_len).unwrap(),
                        }
                    }
                }
                State::ReassemblingRecord { recordbuf, contlen } => {
                    // we're continuing a record, possibly from previous page.
                    let pageleft = self.lsn.remaining_in_block() as u32;

                    // read the rest of the record, or as much as fits on this page.
                    let n = min(contlen.get(), pageleft) as usize;

                    if self.inputbuf.remaining() < n {
                        return Ok(None);
                    }

                    recordbuf.put(self.inputbuf.split_to(n));
                    self.lsn += n as u64;
                    *contlen = match NonZeroU32::new(contlen.get() - n as u32) {
                        Some(x) => x,
                        None => {
                            // The record is now complete.
                            let recordbuf = std::mem::replace(recordbuf, BytesMut::new()).freeze();
                            return Ok(Some(self.complete_record(recordbuf)?));
                        }
                    }
                }
                State::SkippingEverything { skip_until_lsn } => {
                    assert!(*skip_until_lsn >= self.lsn);
                    let n = skip_until_lsn.0 - self.lsn.0;
                    if self.inputbuf.remaining() < n as usize {
                        return Ok(None);
                    }
                    self.inputbuf.advance(n as usize);
                    self.lsn += n;
                    self.state = State::WaitingForRecord;
                }
            }
        }
    }

    fn complete_record(&mut self, recordbuf: Bytes) -> Result<(Lsn, Bytes), WalDecodeError> {
        // We now have a record in the 'recordbuf' local variable.
        // openGauss XLogRecord is 32 bytes:
        //   xl_tot_len:    4 bytes (offset 0)
        //   xl_term:       4 bytes (offset 4)
        //   xl_xid:        8 bytes (offset 8) - TransactionId is uint64 in openGauss
        //   xl_prev:       8 bytes (offset 16)
        //   xl_info:       1 byte  (offset 24)
        //   xl_rmid:       1 byte  (offset 25)
        //   xl_bucket_id:  2 bytes (offset 26)
        //   xl_crc:        4 bytes (offset 28)
        // Total: 32 bytes
        
        // TESTDBG: Print raw record header bytes
        tracing::info!(
            "TESTDBG complete_record: lsn={}, recordbuf.len()={}, header_hex={:02x?}",
            self.lsn,
            recordbuf.len(),
            &recordbuf[0..std::cmp::min(recordbuf.len(), 64)]
        );
        
        let xlogrec =
            XLogRecord::from_slice(&recordbuf[0..XLOG_SIZE_OF_XLOG_RECORD]).map_err(|e| {
                WalDecodeError {
                    msg: format!("xlog record deserialization failed {e}"),
                    lsn: self.lsn,
                }
            })?;

        // TESTDBG: Print parsed XLogRecord fields
        tracing::info!(
            "TESTDBG complete_record: xl_tot_len={}, xl_term={}, xl_xid={}, xl_prev={}, xl_info=0x{:02x}, xl_rmid={}, xl_bucket_id={}, xl_crc=0x{:08x}",
            xlogrec.xl_tot_len,
            xlogrec.xl_term,
            xlogrec.xl_xid,
            xlogrec.xl_prev,
            xlogrec.xl_info,
            xlogrec.xl_rmid,
            xlogrec.xl_bucket_id,
            xlogrec.xl_crc
        );

        // openGauss CRC calculation:
        // CRC is calculated over the record data (after xl_crc) first, then over the header (before xl_crc)
        // xl_crc is at offset 28 in openGauss XLogRecord
        let data_start = XLOG_RECORD_CRC_OFFS + 4; // 32
        let data_end = recordbuf.len();
        let hdr_start = 0;
        let hdr_end = XLOG_RECORD_CRC_OFFS; // 28
        
        tracing::info!(
            "TESTDBG complete_record: CRC calc: data_range=[{}..{}] ({} bytes), hdr_range=[{}..{}] ({} bytes), XLOG_RECORD_CRC_OFFS={}",
            data_start, data_end, data_end - data_start,
            hdr_start, hdr_end, hdr_end - hdr_start,
            XLOG_RECORD_CRC_OFFS
        );
        
        let mut crc = 0;
        crc = crc32c_append(crc, &recordbuf[data_start..data_end]);
        let crc_after_data = crc;
        crc = crc32c_append(crc, &recordbuf[hdr_start..hdr_end]);
        
        tracing::info!(
            "TESTDBG complete_record: CRC calc: crc_after_data=0x{:08x}, final_crc=0x{:08x}, stored_crc=0x{:08x}, match={}",
            crc_after_data, crc, xlogrec.xl_crc, crc == xlogrec.xl_crc
        );
        
        if crc != xlogrec.xl_crc {
            tracing::warn!(
                "WAL record CRC mismatch at {}: computed=0x{:08x}, stored=0x{:08x}, xl_tot_len={}, record_len={}",
                self.lsn,
                crc,
                xlogrec.xl_crc,
                xlogrec.xl_tot_len,
                recordbuf.len()
            );
            return Err(WalDecodeError {
                msg: format!(
                    "WAL record crc mismatch: computed=0x{:08x}, stored=0x{:08x}",
                    crc, xlogrec.xl_crc
                ),
                lsn: self.lsn,
            });
        }

        // XLOG_SWITCH records are special. If we see one, we need to skip
        // to the next WAL segment.
        let next_lsn = if xlogrec.is_xlog_switch_record() {
            trace!("saw xlog switch record at {}", self.lsn);
            self.lsn + self.lsn.calc_padding(WAL_SEGMENT_SIZE as u64)
        } else {
            // openGauss: All records are aligned to MAXALIGN (8 bytes)
            self.lsn.align()
        };
        self.state = State::SkippingEverything {
            skip_until_lsn: next_lsn,
        };

        // We should return LSN of the next record, not the last byte of this record or
        // the byte immediately after. Note that this handles both XLOG_SWITCH and usual
        // records, the former "spans" until the next WAL segment (see test_xlog_switch).
        Ok((next_lsn, recordbuf))
    }
}
