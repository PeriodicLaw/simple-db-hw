package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfield, afield;
    private final Type gbfieldtype;
    private final Op what;
    private HashMap<Field, ArrayList<String>> map;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.afield = afield;
        this.gbfieldtype = gbfieldtype;
        this.what = what;
        this.map = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field gbf = null;
        if (gbfield != NO_GROUPING) {
            assert (tup.getTupleDesc().getFieldType(gbfield) == gbfieldtype);
            gbf = tup.getField(gbfield);
        }
        assert (tup.getTupleDesc().getFieldType(afield) == Type.STRING_TYPE);
        String af = ((StringField) tup.getField(afield)).getValue();
        if (!map.containsKey(gbf))
            map.put(gbf, new ArrayList<>());
        map.get(gbf).add(af);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        return new OpIterator(){
            private static final long serialVersionUID = 1L;
            
            private Iterator<Field> it = null;
            
			public void open() throws DbException, TransactionAbortedException {
				it = map.keySet().iterator();
			}

			public boolean hasNext() throws DbException, TransactionAbortedException {
                return it.hasNext();
			}

			public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                Field f = it.next();
                ArrayList<String> l = map.get(f);
                int agg;
                switch(what){
                    case COUNT:
                        agg = l.size();
						break;
                    default:
                        throw new DbException("not implemented aggregate function "+what.toString()+" on string");
                }
                
                Tuple t = new Tuple(getTupleDesc());
                if(gbfield == NO_GROUPING)
                    t.setField(0, new IntField(agg));
                else{
                    t.setField(0, f);
                    t.setField(1, new IntField(agg));
                }
                return t;
            }

			public void rewind() throws DbException, TransactionAbortedException {
				it = map.keySet().iterator();
			}

			public TupleDesc getTupleDesc() {
                if(gbfield == NO_GROUPING)
                    return new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{what.toString()});
                else
                    return new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE}, new String[]{null, what.toString()});
			}

			public void close() {
				it = null;
			}
            
        };
    }

}
