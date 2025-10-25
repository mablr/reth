//! Common receipts pruning logic.
//!
//! - [`crate::segments::user::Receipts`] is responsible for pruning receipts according to the
//!   user-configured settings (for example, on a full node or with a custom prune config)

use crate::{db_ext::DbTxPruneExt, segments::PruneInput, PrunerError};
use reth_db_api::{table::Value, tables, transaction::DbTxMut};
use reth_primitives_traits::NodePrimitives;
use reth_provider::{
    errors::provider::ProviderResult, BlockReader, DBProvider, NodePrimitivesProvider,
    PruneCheckpointWriter, StaticFileProviderFactory, TransactionsProvider,
};
use reth_prune_types::{PruneCheckpoint, PruneSegment, SegmentOutput, SegmentOutputCheckpoint};
use reth_static_file_types::StaticFileSegment;
use tracing::{debug, trace};

pub(crate) fn prune<Provider>(
    provider: &Provider,
    mut input: PruneInput,
) -> Result<SegmentOutput, PrunerError>
where
    Provider: DBProvider<Tx: DbTxMut>
        + TransactionsProvider
        + BlockReader
        + StaticFileProviderFactory
        + NodePrimitivesProvider<Primitives: NodePrimitives<Receipt: Value>>,
{
    // It is not possible to prune receipts for which we don't have receipt data.
    // If the Receipts checkpoint is lagging behind (which can happen e.g. when
    // pre-merge history is dropped and then later receipt pruning is enabled) then we can
    // only prune from the lowest static file.
    if let Some(lowest_range) =
        provider.static_file_provider().get_lowest_range(StaticFileSegment::Receipts) &&
        input
            .previous_checkpoint
            .is_none_or(|checkpoint| checkpoint.block_number < Some(lowest_range.start()))
    {
        let new_checkpoint = lowest_range.start().saturating_sub(1);
        if let Some(body_indices) = provider.block_body_indices(new_checkpoint)? {
            let prune_mode = input
                .previous_checkpoint
                .map(|c| c.prune_mode)
                .unwrap_or(reth_prune_types::PruneMode::Full);
            input.previous_checkpoint = Some(PruneCheckpoint {
                block_number: Some(new_checkpoint),
                tx_number: Some(body_indices.last_tx_num()),
                prune_mode,
            });
            debug!(
                target: "pruner",
                static_file_checkpoint = ?input.previous_checkpoint,
                "Using static file receipt checkpoint as Receipts starting point"
            );
        }
    }

    let tx_range = match input.get_next_tx_num_range(provider)? {
        Some(range) => range,
        None => {
            trace!(target: "pruner", "No receipts to prune");
            return Ok(SegmentOutput::done())
        }
    };
    let tx_range_end = *tx_range.end();

    let mut limiter = input.limiter;

    let mut last_pruned_transaction = tx_range_end;
    let (pruned, done) = provider.tx_ref().prune_table_with_range::<tables::Receipts<
        <Provider::Primitives as NodePrimitives>::Receipt,
    >>(
        tx_range,
        &mut limiter,
        |_| false,
        |row| last_pruned_transaction = row.0,
    )?;
    trace!(target: "pruner", %pruned, %done, "Pruned receipts from database");

    let last_pruned_block = provider
        .transaction_block(last_pruned_transaction)?
        .ok_or(PrunerError::InconsistentData("Block for transaction is not found"))?
        // If there's more receipts to prune, set the checkpoint block number to previous,
        // so we could finish pruning its receipts on the next run.
        .checked_sub(if done { 0 } else { 1 });

    let progress = limiter.progress(done);

    Ok(SegmentOutput {
        progress,
        pruned,
        checkpoint: Some(SegmentOutputCheckpoint {
            block_number: last_pruned_block,
            tx_number: Some(last_pruned_transaction),
        }),
    })
}

pub(crate) fn save_checkpoint(
    provider: impl PruneCheckpointWriter,
    checkpoint: PruneCheckpoint,
) -> ProviderResult<()> {
    provider.save_prune_checkpoint(PruneSegment::Receipts, checkpoint)?;

    // `PruneSegment::Receipts` overrides `PruneSegment::ContractLogs`, so we can preemptively
    // limit their pruning start point.
    provider.save_prune_checkpoint(PruneSegment::ContractLogs, checkpoint)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::segments::{PruneInput, PruneLimiter, SegmentOutput};
    use alloy_primitives::{BlockNumber, TxNumber, B256};
    use assert_matches::assert_matches;
    use itertools::{
        FoldWhile::{Continue, Done},
        Itertools,
    };
    use reth_db_api::tables;
    use reth_provider::{DBProvider, DatabaseProviderFactory, PruneCheckpointReader};
    use reth_prune_types::{
        PruneCheckpoint, PruneInterruptReason, PruneMode, PruneProgress, PruneSegment,
    };
    use reth_stages::test_utils::{StorageKind, TestStageDB};
    use reth_testing_utils::generators::{
        self, random_block_range, random_receipt, BlockRangeParams,
    };
    use std::ops::Sub;

    #[test]
    fn prune() {
        let db = TestStageDB::default();
        let mut rng = generators::rng();

        let blocks = random_block_range(
            &mut rng,
            1..=10,
            BlockRangeParams { parent: Some(B256::ZERO), tx_count: 2..3, ..Default::default() },
        );
        db.insert_blocks(blocks.iter(), StorageKind::Database(None)).expect("insert blocks");

        let mut receipts = Vec::new();
        for block in &blocks {
            receipts.reserve_exact(block.transaction_count());
            for transaction in &block.body().transactions {
                receipts.push((
                    receipts.len() as u64,
                    random_receipt(&mut rng, transaction, Some(0), None),
                ));
            }
        }
        let receipts_len = receipts.len();
        db.insert_receipts(receipts).expect("insert receipts");

        assert_eq!(
            db.table::<tables::Transactions>().unwrap().len(),
            blocks.iter().map(|block| block.transaction_count()).sum::<usize>()
        );
        assert_eq!(
            db.table::<tables::Transactions>().unwrap().len(),
            db.table::<tables::Receipts>().unwrap().len()
        );

        let test_prune = |to_block: BlockNumber, expected_result: (PruneProgress, usize)| {
            let prune_mode = PruneMode::Before(to_block);
            let mut limiter = PruneLimiter::default().set_deleted_entries_limit(10);
            let input = PruneInput {
                previous_checkpoint: db
                    .factory
                    .provider()
                    .unwrap()
                    .get_prune_checkpoint(PruneSegment::Receipts)
                    .unwrap(),
                to_block,
                limiter: limiter.clone(),
            };

            let next_tx_number_to_prune = db
                .factory
                .provider()
                .unwrap()
                .get_prune_checkpoint(PruneSegment::Receipts)
                .unwrap()
                .and_then(|checkpoint| checkpoint.tx_number)
                .map(|tx_number| tx_number + 1)
                .unwrap_or_default();

            let last_pruned_tx_number = blocks
                .iter()
                .take(to_block as usize)
                .map(|block| block.transaction_count())
                .sum::<usize>()
                .min(
                    next_tx_number_to_prune as usize +
                        input.limiter.deleted_entries_limit().unwrap(),
                )
                .sub(1);

            let provider = db.factory.database_provider_rw().unwrap();
            let result = super::prune(&provider, input).unwrap();
            limiter.increment_deleted_entries_count_by(result.pruned);

            assert_matches!(
                result,
                SegmentOutput {progress, pruned, checkpoint: Some(_)}
                    if (progress, pruned) == expected_result
            );

            super::save_checkpoint(
                &provider,
                result.checkpoint.unwrap().as_prune_checkpoint(prune_mode),
            )
            .unwrap();
            provider.commit().expect("commit");

            let last_pruned_block_number = blocks
                .iter()
                .fold_while((0, 0), |(_, mut tx_count), block| {
                    tx_count += block.transaction_count();

                    if tx_count > last_pruned_tx_number {
                        Done((block.number, tx_count))
                    } else {
                        Continue((block.number, tx_count))
                    }
                })
                .into_inner()
                .0
                .checked_sub(if result.progress.is_finished() { 0 } else { 1 });

            assert_eq!(
                db.table::<tables::Receipts>().unwrap().len(),
                receipts_len - (last_pruned_tx_number + 1)
            );
            assert_eq!(
                db.factory
                    .provider()
                    .unwrap()
                    .get_prune_checkpoint(PruneSegment::Receipts)
                    .unwrap(),
                Some(PruneCheckpoint {
                    block_number: last_pruned_block_number,
                    tx_number: Some(last_pruned_tx_number as TxNumber),
                    prune_mode
                })
            );
        };

        test_prune(
            6,
            (PruneProgress::HasMoreData(PruneInterruptReason::DeletedEntriesLimitReached), 10),
        );
        test_prune(6, (PruneProgress::Finished, 2));
        test_prune(10, (PruneProgress::Finished, 8));
    }

    #[test]
    fn prune_receipts_from_static_files() {
        let db = TestStageDB::default();
        let mut rng = generators::rng();

        // Create blocks with transactions
        let blocks = random_block_range(
            &mut rng,
            0..=10,
            BlockRangeParams { parent: Some(B256::ZERO), tx_count: 2..3, ..Default::default() },
        );

        // Insert blocks into database first
        db.insert_blocks(blocks.iter(), StorageKind::Database(None)).expect("insert blocks");

        // Create receipts for all transactions
        let mut receipts = Vec::new();
        for block in &blocks {
            for transaction in &block.body().transactions {
                receipts.push((
                    receipts.len() as u64,
                    random_receipt(&mut rng, transaction, Some(0), None),
                ));
            }
        }

        // Insert receipts into database
        db.insert_receipts(receipts.clone()).expect("insert receipts");

        // Move receipts to static files
        db.insert_blocks(blocks.iter(), StorageKind::Static).expect("insert into static files");

        // Verify receipts are in the database (before moving to static files exclusively)
        let initial_db_receipts = db.table::<tables::Receipts>().unwrap().len();
        assert_eq!(initial_db_receipts, receipts.len());

        // Test pruning with checkpoint alignment to static files
        let prune_mode = PruneMode::Distance(5); // Keep only last 5 blocks
        let tip_block = blocks.last().unwrap().number;
        let to_block = tip_block.saturating_sub(5);

        let provider = db.factory.database_provider_rw().unwrap();
        let input = PruneInput {
            previous_checkpoint: None,
            to_block,
            limiter: PruneLimiter::default().set_deleted_entries_limit(1000),
        };

        let result = super::prune(&provider, input).unwrap();

        // Verify some receipts were pruned
        assert!(result.pruned > 0, "Expected receipts to be pruned");
        assert_matches!(result.progress, PruneProgress::Finished);

        // Save checkpoint
        super::save_checkpoint(
            &provider,
            result.checkpoint.unwrap().as_prune_checkpoint(prune_mode),
        )
        .unwrap();
        provider.commit().expect("commit");

        // Verify checkpoint was saved correctly
        let checkpoint =
            db.factory.provider().unwrap().get_prune_checkpoint(PruneSegment::Receipts).unwrap();
        assert!(checkpoint.is_some(), "Checkpoint should be saved");
        assert!(checkpoint.unwrap().block_number.is_some(), "Checkpoint should have block number");
    }
}
