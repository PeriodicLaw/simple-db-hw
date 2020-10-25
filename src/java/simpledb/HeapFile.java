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
        RandomAccessFile raf = new RandomAccessFile(f, "rw");
        int pagesize = BufferPool.getPageSize();
        raf.seek(page.getId().getPageNumber() * pagesize);
        raf.write(page.getPageData(), 0, pagesize);
        raf.close();
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
        for(int i=0; i<numPages(); i++){
            HeapPage p = (HeapPage)Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i), Permissions.READ_WRITE);
            if(p.getNumEmptySlots() > 0){
                p.insertTuple(t);
                ArrayList<Page> l = new ArrayList<Page>();
                l.add(p);
                return l;
            }
        }
        
        // add a new page, but still get it through BufferPool
        BufferedOutputStream bw = new BufferedOutputStream(new FileOutputStream(f, true));
        byte[] emptyData = HeapPage.createEmptyPageData();
        bw.write(emptyData);
        bw.close();
        
        HeapPage p = (HeapPage)Database.getBufferPool().getPage(tid, new HeapPageId(getId(), numPages()-1), Permissions.READ_WRITE);
        p.insertTuple(t);
        writePage(p);
        ArrayList<Page> l = new ArrayList<Page>();
        l.add(p);
        return l;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        HeapPage p = (HeapPage)Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        p.deleteTuple(t);
        ArrayList<Page> l = new ArrayList<Page>();
        l.add(p);
        return l;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new DbFileIterator(){
            private TransactionId tid;
            private int pageno = 0;
            private HeapPage p;
            private Iterator<Tuple> pgit;
            
            public void open() throws DbException, TransactionAbortedException {
                tid = new TransactionId();
                pageno = 0;
                p = (HeapPage)Database.getBufferPool().getPage(tid, new HeapPageId(getId(), pageno), Permissions.READ_ONLY);
                pgit = p.iterator();
            }
            
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if(tid == null || pageno == numPages()) return false;
                while(!pgit.hasNext()){
                    pageno += 1;
                    if(pageno == numPages()) return false;
                    p = (HeapPage)Database.getBufferPool().getPage(tid, new HeapPageId(getId(), pageno), Permissions.READ_ONLY);
                    pgit = p.iterator();
                }
                return true;
            }

            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if(!hasNext())
                    throw new NoSuchElementException();
                return pgit.next();
            }

            public void rewind() throws DbException, TransactionAbortedException {
                pageno = 0;
                p = (HeapPage)Database.getBufferPool().getPage(tid, new HeapPageId(getId(), pageno), Permissions.READ_ONLY);
                pgit = p.iterator();
            }

            public void close() {
                tid = null;
                pageno = 0;
                p = null;
                pgit = null;
            }
        };
    }

}

