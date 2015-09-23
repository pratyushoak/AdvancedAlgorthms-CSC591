
/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package storm.starter.trident.project.countmin.state;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import java.util.Arrays;
import java.util.Random;


import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.StateType;

//import comparator, priority and iterator queue utilities
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Iterator;

//import com.clearspring.analytics.stream.membership.Filter;
//import Filter;

/**
 * Count-Min Sketch datastructure.
 * An Improved Data Stream Summary: The Count-Min Sketch and its Applications
 * http://www.eecs.harvard.edu/~michaelm/CS222/countmin.pdf
 * Modified by Preetham MS. Originally by https://github.com/addthis/stream-lib/
 * Preetham MS (pmahish@ncsu.edu)
 *@modified by Pratyush Oak
 */
public class CountMinSketchState implements State {

    public static final long PRIME_MODULUS = (1L << 31) - 1;

    int depth;
    int width;
    long[][] table;
    long[] hashA;
    long size;
    double eps;
    double confidence;
    //add private varaibles to hold the value of "k" and the priority queue
    int k;
    PriorityQueue<String> pQueue;
    //create a new comparator object to compare counts of words
    Comparator<String> comparator= new SketchComparator();

    CountMinSketchState() {
    }

    public CountMinSketchState(int depth, int width, int seed, int k) {
        System.out.println("\nin CountMinSketchState\n");
        this.depth = depth;
        this.width = width;
        this.eps = 2.0 / width;
        this.confidence = 1 - 1 / Math.pow(2, depth);
        initTablesWith(depth, width, seed);
        //initialize the value of k and create a priority queue of size k and the comparator function
        this.k=k;
        pQueue=new PriorityQueue<String>(this.k,comparator);
    }

    public CountMinSketchState(double epsOfTotalCount, double confidence, int seed) {
        // 2/w = eps ; w = 2/eps
        // 1/2^depth <= 1-confidence ; depth >= -log2 (1-confidence)
        this.eps = epsOfTotalCount;
        this.confidence = confidence;
        this.width = (int) Math.ceil(2 / epsOfTotalCount);
        this.depth = (int) Math.ceil(-Math.log(1 - confidence) / Math.log(2));
        initTablesWith(depth, width, seed);
    }

    public CountMinSketchState(int depth, int width, int size, long[] hashA, long[][] table) {
        this.depth = depth;
        this.width = width;
        this.eps = 2.0 / width;
        this.confidence = 1 - 1 / Math.pow(2, depth);
        this.hashA = hashA;
        this.table = table;
        this.size = size;
    }

    private void initTablesWith(int depth, int width, int seed) {
        this.table = new long[depth][width];
        this.hashA = new long[depth];
        Random r = new Random(seed);
        // We're using a linear hash functions
        // of the form (a*x+b) mod p.
        // a,b are chosen independently for each hash function.
        // However we can set b = 0 as all it does is shift the results
        // without compromising their uniformity or independence with
        // the other hashes.
        for (int i = 0; i < depth; ++i) {
            hashA[i] = r.nextInt(Integer.MAX_VALUE);
        }
    }

//comparator class to compare counts of words to decide whether
//a word is added to the priority queue or not
public class SketchComparator implements Comparator<String>
    {
        @Override
        public int compare(String s1, String s2)
        {   
            int retValue= (int)((estimateCount(s1))-(estimateCount(s2)));
            return retValue;
        }
    }

    public double getRelativeError() {
        return eps;
    }

    public double getConfidence() {
        return confidence;
    }

    int hash(long item, int i) {
        long hash = hashA[i] * item;
        // A super fast way of computing x mod 2^p-1
        // See http://www.cs.princeton.edu/courses/archive/fall09/cos521/Handouts/universalclasses.pdf
        // page 149, right after Proposition 7.
        hash += hash >> 32;
        hash &= PRIME_MODULUS;
        // Doing "%" after (int) conversion is ~2x faster than %'ing longs.
        return ((int) hash) % width;
    }

    
    public void add(long item, long count) {
        if (count < 0) {
            // Actually for negative increments we'll need to use the median
            // instead of minimum, and accuracy will suffer somewhat.
            // Probably makes sense to add an "allow negative increments"
            // parameter to constructor.
            throw new IllegalArgumentException("Negative increments not implemented");
        }
        for (int i = 0; i < depth; ++i) {
            table[i][hash(item, i)] += count;
        }
        size += count;
       }

    
    public void add(String item, long count) {
        if (count < 0) {
            // Actually for negative increments we'll need to use the median
            // instead of minimum, and accuracy will suffer somewhat.
            // Probably makes sense to add an "allow negative increments"
            // parameter to constructor.
            throw new IllegalArgumentException("Negative increments not implemented");
        }
        int[] buckets = Filter.getHashBuckets(item, depth, width);
        for (int i = 0; i < depth; ++i) {
            table[i][buckets[i]] += count;
        }
        size += count;
    //when a string item is added to the count min data structure, update the priority queue
     updateQueue(item);
    
    }
    //function to update the priority queue if needed when a new word is added to countmin
    public void updateQueue(String s)
    {
        
        int size=pQueue.size();
        String prevHead;
        //System.out.println("\nUPDATING queue size : "+size+" k:"+k+"\n");
        
        if(pQueue.contains(s)) // if it already contains the input item
            { 
            pQueue.remove(s);   // remove it
            }
        // if there is place in the queue for the new word insert it
        if(pQueue.size()<k)
        {   
            // add it again
            pQueue.offer(s); 
            //System.out.println("\n head:"+pQueue.peek()+" count: "+estimateCount(pQueue.peek()));

        }
        else
        {   // if count of new word more than count of head(head has lowest count among priority queue elements)
            //estimate count gets the count of the words, which is stored in the countmin data structure 
            if(estimateCount(s)>estimateCount(pQueue.peek()))
            {   
                
                //System.out.println("\n queue: size <> k\n");
                //System.out.println("\n head:"+pQueue.peek()+" count: "+estimateCount(pQueue.peek()));                prevHead=pQueue.poll();// remove old head
                //remove the head
                pQueue.poll();
                // add new item
                pQueue.offer(s); 
            }
        }
    }
    //function to return the top-k words and their counts
    public String returnPeekOFQueue()
    {   
        //initialize string to hold top-k
        String s="Top-k:";
        //System.out.println("\nin ret pQ:"+this.pQueue.peek()+"\n");
        //create an interator to iterate throught the priority queue
        Iterator it = pQueue.iterator();
        //loop through the priority queue
        while(it.hasNext())
        {
            //hold the nect string in the queue
            String temp = it.next().toString();
            //append the string in the queue and its count to the main string
            s+= "["+temp + ",Count:" + estimateCount(temp) + "]";
        }
        //System.out.println("\n queue size:"+this.pQueue.size()+"\n")
        return s;
    }

    public long size() {
        return size;
    }

    /**
     * The estimate is correct within 'epsilon' * (total item count),
     * with probability 'confidence'.
     */
    
    public long estimateCount(long item) {
        long res = Long.MAX_VALUE;
        for (int i = 0; i < depth; ++i) {
            res = Math.min(res, table[i][hash(item, i)]);
        }
        return res;
    }

    
    public long estimateCount(String item) {
        long res = Long.MAX_VALUE;
        int[] buckets = Filter.getHashBuckets(item, depth, width);
        for (int i = 0; i < depth; ++i) {
            res = Math.min(res, table[i][buckets[i]]);
        }
        return res;
    }

    /**
     * Merges count min sketches to produce a count min sketch for their combined streams
     *
     * @param estimators
     * @return merged estimator or null if no estimators were provided
     * @throws CMSMergeException if estimators are not mergeable (same depth, width and seed)
     */
    public static CountMinSketchState merge(CountMinSketchState... estimators) throws CMSMergeException {
        CountMinSketchState merged = null;
        if (estimators != null && estimators.length > 0) {
            int depth = estimators[0].depth;
            int width = estimators[0].width;
            long[] hashA = Arrays.copyOf(estimators[0].hashA, estimators[0].hashA.length);

            long[][] table = new long[depth][width];
            int size = 0;

            for (CountMinSketchState estimator : estimators) {
                if (estimator.depth != depth) {
                    throw new CMSMergeException("Cannot merge estimators of different depth");
                }
                if (estimator.width != width) {
                    throw new CMSMergeException("Cannot merge estimators of different width");
                }
                if (!Arrays.equals(estimator.hashA, hashA)) {
                    throw new CMSMergeException("Cannot merge estimators of different seed");
                }

                for (int i = 0; i < table.length; i++) {
                    for (int j = 0; j < table[i].length; j++) {
                        table[i][j] += estimator.table[i][j];
                    }
                }
                size += estimator.size;
            }

            merged = new CountMinSketchState(depth, width, size, hashA, table);
        }

        return merged;
    }

    public static byte[] serialize(CountMinSketchState sketch) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream s = new DataOutputStream(bos);
        try {
            s.writeLong(sketch.size);
            s.writeInt(sketch.depth);
            s.writeInt(sketch.width);
            for (int i = 0; i < sketch.depth; ++i) {
                s.writeLong(sketch.hashA[i]);
                for (int j = 0; j < sketch.width; ++j) {
                    s.writeLong(sketch.table[i][j]);
                }
            }
            return bos.toByteArray();
        } catch (IOException e) {
            // Shouldn't happen
            throw new RuntimeException(e);
        }
    }

    public static CountMinSketchState deserialize(byte[] data) {
        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        DataInputStream s = new DataInputStream(bis);
        try {
            CountMinSketchState sketch = new CountMinSketchState();
            sketch.size = s.readLong();
            sketch.depth = s.readInt();
            sketch.width = s.readInt();
            sketch.eps = 2.0 / sketch.width;
            sketch.confidence = 1 - 1 / Math.pow(2, sketch.depth);
            sketch.hashA = new long[sketch.depth];
            sketch.table = new long[sketch.depth][sketch.width];
            for (int i = 0; i < sketch.depth; ++i) {
                sketch.hashA[i] = s.readLong();
                for (int j = 0; j < sketch.width; ++j) {
                    sketch.table[i][j] = s.readLong();
                }
            }
            return sketch;
        } catch (IOException e) {
            // Shouldn't happen
            throw new RuntimeException(e);
        }
    }

    @Override
    public void beginCommit(Long txid) {
        return;
    }

    @Override
    public void commit(Long txid) {
        return;
    }

    @SuppressWarnings("serial")
    protected static class CMSMergeException extends RuntimeException {
   // protected static class CMSMergeException extends FrequencyMergeException {

        public CMSMergeException(String message) {
            super(message);
        }
    }
}
