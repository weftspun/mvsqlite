use std::str::FromStr;

use anyhow::{Context, Result};
use foundationdb::{options::TransactionOption, Transaction};

use crate::{
    delta::reader::DeltaReader, fixed::FixedString, page::Page, util::decode_version, Core,
};

/// A page read request. Mirrors the old `/batch/read` wire request, minus
/// the wire-only `accept_zstd` toggle (there's no wire in Direct mode - the
/// caller gets the decoded page bytes directly).
pub struct ReadPageRequest<'a> {
    pub page_index: u32,
    pub version: &'a str,
    /// If set, do a read-your-writes lookup by content hash instead of a
    /// versioned read - used when the caller already has a page buffered
    /// locally and wants to confirm/refresh it within the same transaction.
    pub hash: Option<[u8; 32]>,
}

impl Core {
    /// Mirrors mvstore's old `handle_read_req`: if `req.hash` is given, does
    /// a read-your-writes lookup by content hash (ignoring the FDB read
    /// version); otherwise does a normal versioned read, falling back to the
    /// namespace's `overlay_base` if the page isn't found there. Returns the
    /// decoded, uncompressed page.
    pub async fn read_page(&self, ns_id: [u8; 10], req: ReadPageRequest<'_>) -> Result<Option<Page>> {
        if let Some(hash) = req.hash {
            let txn = self.db.create_trx()?;
            if self.is_read_only() {
                txn.set_option(TransactionOption::ReadLockAware).unwrap();
            }

            let reader = DeltaReader {
                txn: &txn,
                ns_id,
                key_codec: &self.key_codec,
                replica_manager: self.replica_manager.as_ref(),
                content_cache: self.content_cache.as_ref(),
            };
            let content = reader
                .get_page_content_decoded_snapshot_compressed(hash)
                .await
                .with_context(|| "failed to get content by hash")?;
            Ok(content.map(|x| Page {
                version: FixedString::from_str(req.version).unwrap_or_default(),
                data: x,
            }))
        } else {
            let txn = self
                .create_versioned_read_txn(req.version)
                .await
                .with_context(|| "failed to create versioned read txn")?;
            let mut page = self
                .read_page_decoded_snapshot_compressed(&txn, ns_id, req.page_index, req.version)
                .await
                .with_context(|| "error reading page")?;

            if page.is_none() {
                let metadata = self
                    .ns_metadata_cache
                    .get(&txn, &self.key_codec, ns_id)
                    .await
                    .map_err(|e| *e)?;
                if let Some(base) = &metadata.overlay_base {
                    let base_ns_id = decode_version(&base.ns_id)?;
                    page = self
                        .read_page_decoded_snapshot_compressed(
                            &txn,
                            base_ns_id,
                            req.page_index,
                            &base.snapshot_version,
                        )
                        .await?;
                }
            }

            Ok(page)
        }
    }

    async fn read_page_decoded_snapshot_compressed(
        &self,
        txn: &Transaction,
        ns_id: [u8; 10],
        page_index: u32,
        page_version_hex: &str,
    ) -> Result<Option<Page>> {
        let reader = DeltaReader {
            txn,
            ns_id,
            key_codec: &self.key_codec,
            replica_manager: self.replica_manager.as_ref(),
            content_cache: self.content_cache.as_ref(),
        };
        let (version, hash) = match reader
            .read_page_hash(page_index, Some(page_version_hex), true)
            .await?
        {
            Some(x) => x,
            None => {
                tracing::debug!(
                    page_index,
                    requested_version = page_version_hex,
                    "read_page_decoded_snapshot_compressed: no page hash found"
                );
                return Ok(None);
            }
        };
        tracing::debug!(
            page_index,
            requested_version = page_version_hex,
            found_version = version.as_str(),
            hash = hex::encode(hash),
            "read_page_decoded_snapshot_compressed: found page hash"
        );
        let data = reader
            .get_page_content_decoded_snapshot_compressed(hash)
            .await?
            .with_context(|| "cannot find content for the provided hash")?;
        Ok(Some(Page { version, data }))
    }
}
