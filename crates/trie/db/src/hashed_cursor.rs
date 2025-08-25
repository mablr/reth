use alloy_primitives::{B256, U256};
use parking_lot::Mutex;
use reth_db_api::{
    cursor::{DbCursorRO, DbDupCursorRO},
    tables,
    transaction::DbTx,
    DatabaseError,
};
use reth_primitives_traits::Account;
use reth_trie::hashed_cursor::{HashedCursor, HashedCursorFactory, HashedStorageCursor};
use std::{fmt, sync::Arc};

/// A struct wrapping database transaction that implements [`HashedCursorFactory`].
///
/// It caches and shares a single `HashedStorages` dup cursor across calls to
/// `hashed_storage_cursor` within the same factory, to avoid repeatedly creating
/// new database cursor handles during a state root run.
pub struct DatabaseHashedCursorFactory<'a, TX>
where
    TX: DbTx,
{
    tx: &'a TX,
    /// Shared dup-read cursor over `HashedStorages` reused across calls.
    shared_storage_cursor: Arc<Mutex<<TX as DbTx>::DupCursor<tables::HashedStorages>>>,
}

impl<TX> fmt::Debug for DatabaseHashedCursorFactory<'_, TX>
where
    TX: DbTx,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DatabaseHashedCursorFactory").finish()
    }
}

impl<TX> Clone for DatabaseHashedCursorFactory<'_, TX>
where
    TX: DbTx,
{
    fn clone(&self) -> Self {
        Self { tx: self.tx, shared_storage_cursor: self.shared_storage_cursor.clone() }
    }
}

impl<'a, TX> DatabaseHashedCursorFactory<'a, TX>
where
    TX: DbTx,
{
    /// Create new database hashed cursor factory.
    pub fn new(tx: &'a TX) -> Self {
        let cursor = tx
            .cursor_dup_read::<tables::HashedStorages>()
            .expect("dup cursor for HashedStorages must be creatable");
        Self { tx, shared_storage_cursor: Arc::new(Mutex::new(cursor)) }
    }
}

impl<TX: DbTx> HashedCursorFactory for DatabaseHashedCursorFactory<'_, TX> {
    type AccountCursor = DatabaseHashedAccountCursor<<TX as DbTx>::Cursor<tables::HashedAccounts>>;
    type StorageCursor =
        DatabaseHashedStorageCursor<<TX as DbTx>::DupCursor<tables::HashedStorages>>;

    fn hashed_account_cursor(&self) -> Result<Self::AccountCursor, DatabaseError> {
        Ok(DatabaseHashedAccountCursor(self.tx.cursor_read::<tables::HashedAccounts>()?))
    }

    fn hashed_storage_cursor(
        &self,
        hashed_address: B256,
    ) -> Result<Self::StorageCursor, DatabaseError> {
        Ok(DatabaseHashedStorageCursor::new(self.shared_storage_cursor.clone(), hashed_address))
    }
}

/// A struct wrapping database cursor over hashed accounts implementing [`HashedCursor`] for
/// iterating over accounts.
#[derive(Debug)]
pub struct DatabaseHashedAccountCursor<C>(C);

impl<C> DatabaseHashedAccountCursor<C> {
    /// Create new database hashed account cursor.
    pub const fn new(cursor: C) -> Self {
        Self(cursor)
    }
}

impl<C> HashedCursor for DatabaseHashedAccountCursor<C>
where
    C: DbCursorRO<tables::HashedAccounts>,
{
    type Value = Account;

    fn seek(&mut self, key: B256) -> Result<Option<(B256, Self::Value)>, DatabaseError> {
        self.0.seek(key)
    }

    fn next(&mut self) -> Result<Option<(B256, Self::Value)>, DatabaseError> {
        self.0.next()
    }
}

/// The structure wrapping a database cursor for hashed storage and
/// a target hashed address. Implements [`HashedCursor`] and [`HashedStorageCursor`]
/// for iterating over hashed storage.
#[derive(Debug, Clone)]
pub struct DatabaseHashedStorageCursor<C> {
    /// Database hashed storage cursor.
    cursor: Arc<Mutex<C>>,
    /// Target hashed address of the account that the storage belongs to.
    hashed_address: B256,
}

impl<C> DatabaseHashedStorageCursor<C> {
    /// Create new [`DatabaseHashedStorageCursor`].
    pub const fn new(cursor: Arc<Mutex<C>>, hashed_address: B256) -> Self {
        Self { cursor, hashed_address }
    }
}

impl<C> HashedCursor for DatabaseHashedStorageCursor<C>
where
    C: DbCursorRO<tables::HashedStorages> + DbDupCursorRO<tables::HashedStorages>,
{
    type Value = U256;

    fn seek(&mut self, subkey: B256) -> Result<Option<(B256, Self::Value)>, DatabaseError> {
        Ok(self
            .cursor
            .lock()
            .seek_by_key_subkey(self.hashed_address, subkey)?
            .map(|e| (e.key, e.value)))
    }

    fn next(&mut self) -> Result<Option<(B256, Self::Value)>, DatabaseError> {
        Ok(self.cursor.lock().next_dup_val()?.map(|e| (e.key, e.value)))
    }
}

impl<C> HashedStorageCursor for DatabaseHashedStorageCursor<C>
where
    C: DbCursorRO<tables::HashedStorages> + DbDupCursorRO<tables::HashedStorages>,
{
    fn is_storage_empty(&mut self) -> Result<bool, DatabaseError> {
        Ok(self.cursor.lock().seek_exact(self.hashed_address)?.is_none())
    }
}
