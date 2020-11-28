package simpledb;

import java.io.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    
    private int maxNumPages;
    private ConcurrentHashMap<PageId, Page> pages;
    
    private interface Lock {
    }
    private class SharedLock implements Lock {
        HashSet<TransactionId> readers;
        SharedLock(HashSet<TransactionId> readers){this.readers = readers;}
    }
    private class ExclusiveLock implements Lock {
        TransactionId owner;
        ExclusiveLock(TransactionId owner){this.owner = owner;}
    }
    private ConcurrentHashMap<PageId, Lock> locks;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.maxNumPages = numPages;
        pages = new ConcurrentHashMap<>();
        locks = new ConcurrentHashMap<>();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }
    
    private void insertPage(Page p) throws DbException{
        if(pages.size() == maxNumPages)
            evictPage();
        pages.put(p.getId(), p);
    }
    
    private static final long MAX_TIMEOUT = 1000;
    
    private void gainSharedLock(TransactionId tid, PageId pid) throws TransactionAbortedException {
        long timeout = System.currentTimeMillis() + (long)(Math.random() * MAX_TIMEOUT);
        while(true){
            synchronized(locks) {
                if(locks.get(pid) == null) {
                    HashSet<TransactionId> readers = new HashSet<>();
                    readers.add(tid);
                    locks.put(pid, new SharedLock(readers));
                    return;
                } else if(locks.get(pid) instanceof SharedLock){
                    SharedLock lock = (SharedLock)locks.get(pid);
                    lock.readers.add(tid);
                    return;
                } else {
                    ExclusiveLock lock = (ExclusiveLock)locks.get(pid);
                    if(lock.owner == tid)
                        return;
                }
            }
            
            if(System.currentTimeMillis() > timeout){
                throw new TransactionAbortedException();
            }
        }
    }
    
    private void gainExclusiveLock(TransactionId tid, PageId pid) throws TransactionAbortedException {
        // System.out.printf("gain exclusive lock %d %s\n", tid.myid, pid.toString());
        long timeout = System.currentTimeMillis() + (long)(Math.random() * MAX_TIMEOUT);
        while(true){
            synchronized(locks) {
                if(locks.get(pid) == null) {
                    locks.put(pid, new ExclusiveLock(tid));
                    return;
                } else if(locks.get(pid) instanceof SharedLock){
                    SharedLock lock = (SharedLock)locks.get(pid);
                    if(lock.readers.size() == 1 && lock.readers.contains(tid)){
                        locks.remove(pid);
                        locks.put(pid, new ExclusiveLock(tid));
                        return;
                    }
                } else if(locks.get(pid) instanceof ExclusiveLock){
                    ExclusiveLock lock = (ExclusiveLock)locks.get(pid);
                    if(lock.owner == tid)
                        return;
                }
            }
            
            
            if(System.currentTimeMillis() > timeout){
                throw new TransactionAbortedException();
            }
        }
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        
        if(perm == Permissions.READ_ONLY)
            gainSharedLock(tid, pid);
        else if(perm == Permissions.READ_WRITE)
            gainExclusiveLock(tid, pid);
            
        Page p = pages.get(pid);
        if(p == null){
            DbFile f = Database.getCatalog().getDatabaseFile(pid.getTableId());
            p = f.readPage(pid);
            insertPage(p);
        }
        return p;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        Lock lock = locks.get(pid);
        assert(lock != null);
        if(lock instanceof SharedLock){
            SharedLock slock = (SharedLock)lock;
            slock.readers.remove(tid);
            if(slock.readers.isEmpty())
                locks.remove(pid);
        }else{
            ExclusiveLock elock = (ExclusiveLock)lock;
            assert(elock.owner == tid);
            locks.remove(pid);
        }
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        Lock lock = locks.get(p);
        if(lock == null)
            return false;
        else if(lock instanceof SharedLock)
            return ((SharedLock)lock).readers.contains(tid);
        else
            return ((ExclusiveLock)lock).owner == tid;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        if(commit)
            flushPages(tid);
        
        synchronized(locks){
            for(Map.Entry<PageId, Lock> entry: locks.entrySet()){
                Lock lock = entry.getValue();
                if(lock instanceof SharedLock){
                    ((SharedLock)lock).readers.remove(tid);
                    if(((SharedLock)lock).readers.size() == 0){
                        locks.remove(entry.getKey());
                        discardPage(entry.getKey());
                    }
                }else if(lock instanceof ExclusiveLock && ((ExclusiveLock)lock).owner == tid){
                    locks.remove(entry.getKey());
                    discardPage(entry.getKey());
                }
            }
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        DbFile f = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> l = f.insertTuple(tid, t);
        for(Page p: l){
            p.markDirty(true, tid);
            if(!pages.containsKey(p.getId()))
                insertPage(p);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        DbFile f = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        ArrayList<Page> l = f.deleteTuple(tid, t);
        for(Page p: l){
            p.markDirty(true, tid);
            if(!pages.containsKey(p.getId()))
                insertPage(p);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for(Page p: pages.values()){
            DbFile f = Database.getCatalog().getDatabaseFile(p.getId().getTableId());
            f.writePage(p);
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        pages.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        Page p = pages.get(pid);
        if(p == null || p.isDirty() == null)
            return;
        DbFile f = Database.getCatalog().getDatabaseFile(pid.getTableId());
        f.writePage(p);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        synchronized(locks){
            for(Map.Entry<PageId, Lock> entry: locks.entrySet()){
                Lock lock = entry.getValue();
                if(lock instanceof SharedLock){
                    HashSet<TransactionId> readers = ((SharedLock)lock).readers;
                    if(readers.size() == 1 && readers.contains(tid))
                        flushPage(entry.getKey());
                }else if(lock instanceof ExclusiveLock && ((ExclusiveLock)lock).owner == tid)
                    flushPage(entry.getKey());
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        if(!pages.isEmpty()){
            for(Map.Entry<PageId, Page> entry: pages.entrySet())
                if(entry.getValue().isDirty() == null){
                    discardPage(entry.getKey());
                    return;
                }
            throw new DbException("too many dirty pages");
        }
    }
}
