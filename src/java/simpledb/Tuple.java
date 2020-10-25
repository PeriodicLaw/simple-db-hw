package simpledb;

import java.io.Serializable;
// import java.util.Arrays;
import java.util.Vector;
import java.util.Iterator;

// import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    
    private TupleDesc td;
    private Vector<Field> fields;
    private RecordId rid;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        this.td = td;
        fields = new Vector<Field>();
        for(int i=0; i<td.numFields(); i++)
            fields.add(null);
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        return rid;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        this.rid = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        fields.set(i, f);
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        return fields.get(i);
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        String s = "";
        for(int i=0; i<fields.size(); i++){
            if(i>0) s += "\t";
            s += fields.get(i);
        }
        return s;
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        return fields.iterator();
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        this.td = td;
    }
    
    public static Tuple merge(Tuple t1, Tuple t2){
        Tuple t = new Tuple(TupleDesc.merge(t1.getTupleDesc(), t2.getTupleDesc()));
        for(int i=0; i<t1.getTupleDesc().numFields(); i++)
            t.setField(i, t1.getField(i));
        for(int i=0; i<t2.getTupleDesc().numFields(); i++)
            t.setField(i + t1.getTupleDesc().numFields(), t2.getField(i));
        return t;
    }
}
