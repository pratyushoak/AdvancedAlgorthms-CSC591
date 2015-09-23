package storm.starter.trident.project.countmin.state;

import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;
import storm.trident.operation.TridentCollector;
import java.util.List;
import java.util.ArrayList;
import backtype.storm.tuple.Values;
import java.util.*;
import storm.starter.trident.project.countmin.state.CountMinSketchState;
/*
@author: Pratyush Oak
*/

public class CountMin extends BaseQueryFunction<CountMinSketchState, String> {
    public List<String> batchRetrieve(CountMinSketchState state, List<TridentTuple> inputs) {
    //list will hold the top k words
    List<String> top_k_list = new ArrayList();
    String s;
    //System.out.println("\ninside CountMin\n");
    //returns the top k words and their count as a string
    s = state.returnPeekOFQueue();
    // we add the resulting string to a list 
    top_k_list.add(s);
    //return the list
    return top_k_list;    
}
    public void execute(TridentTuple tuple, String count, TridentCollector collector) {
        collector.emit(new Values(count));
    }    
}
