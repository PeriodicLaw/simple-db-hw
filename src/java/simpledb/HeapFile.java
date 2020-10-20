package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    
    private final File f;
    private final TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return f.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        try {
            RandomAccessFile raf = new RandomAccessFile(f, "r");
            int pagesize = BufferPool.getPageSize();
            byte[] b = new byte[pagesize];
            raf.seek(pid.getPageNumber() * pagesize);
            raf.read(b, 0, pagesize);
            raf.close();
            return new HeapPage((HeapPageId)pid, b);
        } catch(Exception e) {
            e.printStackTrace();
            throw new IllegalArgumentException();
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int)(f.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }
    
    private class itr implements DbFileIterator{
        private final HeapFile f;
        
        private TransactionId tid;
        private HeapPageId pid;
        private HeapPage p;
        private Iterator<Tuple> pgit;
        
        public itr(HeapFile f){
            this.f = f;
        }
        
        public void open() throws DbException, TransactionAbortedException {
            tid = new TransactionId();
            pid = new HeapPageId(f.getId(), 0);
            p = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY); // TODO read_write permission
            pgit = p.iterator();
        }

        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(tid == null) return false;
            return (pid.getPageNumber() != (f.numPages()-1)) || pgit.hasNext();
        }

        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(!hasNext())
                throw new NoSuchElementException();
            while(!pgit.hasNext()){
                pid = new HeapPageId(f.getId(), pid.getPageNumber() + 1);
                p = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY); // TODO read_write permission
                pgit = p.iterator();
            }
            return pgit.next();
        }

        public void rewind() throws DbException, TransactionAbortedException {
            pid = new HeapPageId(f.getId(), 0);
            p = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY); // TODO read_write permission
            pgit = p.iterator();
        }

        public void close() {
            tid = null;
            pid = null;
            p = null;
            pgit = null;
        }
        
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new itr(this);
    }

}

