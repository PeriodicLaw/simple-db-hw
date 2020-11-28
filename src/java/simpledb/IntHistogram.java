package simpledb;

import java.util.ArrayList;

import simpledb.Predicate.Op;

/**
 * A class to represent a fixed-width histogram over a single integer-based
 * field.
 */
public class IntHistogram {
    
    private final int buckets, min, max;
    private final ArrayList<Integer> hist;
    private int total_cnt;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        assert(max > min);
        this.buckets = (max-min+1 < buckets) ? (max-min+1) : buckets;
        this.min = min;
        this.max = max;
        
        total_cnt = 0;
        hist = new ArrayList<>();
        for(int i=0; i<buckets; i++)
            hist.add(0);
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        if(max > min){
            int i = buckets*(v-min)/(max-min+1);
            hist.set(i, hist.get(i)+1);
        }
        total_cnt += 1;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        if(v<min)
            return (op == Op.GREATER_THAN || op == Op.GREATER_THAN_OR_EQ || op == Op.NOT_EQUALS) ? 1.0 : 0.0;
        if(v>max)
            return (op == Op.LESS_THAN || op == Op.LESS_THAN_OR_EQ || op == Op.NOT_EQUALS) ? 1.0 : 0.0;
        assert(total_cnt > 0);
        
        int i = buckets*(v-min)/(max-min+1),
            imin = min + i*(max-min+1)/buckets,
            imax = min + (i+1)*(max-min+1)/buckets;
        
        double cnt;
        switch(op){
            case EQUALS:
                return hist.get(i)*1.0/(total_cnt*(imax-imin));
            case GREATER_THAN:
                cnt = hist.get(i)*(imax-v-1)/(imax-imin);
                for(int j=i+1; j<buckets; j++)
                    cnt += hist.get(j);
                return cnt/total_cnt;
            case GREATER_THAN_OR_EQ:
                cnt = hist.get(i)*(imax-v)/(imax-imin);
                for(int j=i+1; j<buckets; j++)
                    cnt += hist.get(j);
                return cnt/total_cnt;
            case LESS_THAN:
                cnt = hist.get(i)*(v-imin)/(imax-imin);
                for(int j=0; j<i; j++)
                    cnt += hist.get(j);
                return cnt/total_cnt;
            case LESS_THAN_OR_EQ:
                cnt = hist.get(i)*(v-imin+1)/(imax-imin);
                for(int j=0; j<i; j++)
                    cnt += hist.get(j);
                return cnt/total_cnt;
            case NOT_EQUALS:
                return cnt = 1.0 - hist.get(i)*1.0/(total_cnt*(imax-imin));
            default:
                return -1.0;
        }
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
