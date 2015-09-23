package storm.starter.trident.tutorial;
import java.io.IOException;
import storm.trident.TridentTopology;
import storm.trident.Stream;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
//------------------------------------
// Step 1: broken into two lines to fit margins
import storm.starter.trident.tutorial.spouts.FakeTweetsBatchSpout;
import storm.starter.trident.tutorial.filters.PrintFilter;
import storm.starter.trident.tutorial.filters.RegexFilter;
import storm.starter.trident.tutorial.functions.ToUpperCaseFunction;
import storm.trident.operation.builtin.Count;
import storm.trident.testing.MemoryMapState;

public class AggregatorsTopology {
    // -----------------------------------------
    // Step 2: Specify which file spout
    // should read from (if any).
    // Data path relative to pom.xml file.
    //------------------------------------------
private static final String DATA_PATH = "data/500_sentences_en.txt";

public static StormTopology buildTopology() throws IOException {
// Step 3: broken into two lines to fit margins
Fields inputFields = new Fields("id", "text", "actor", "location", "date");
// Step 4: broken into two lines to fit margins
FakeTweetsBatchSpout spout = new FakeTweetsBatchSpout(DATA_PATH);
// Step 5: Define filters to apply to the input fields
//--------------------------------------------------------
//---------------------------------------------------------
PrintFilter filter = new PrintFilter();
// Step 6: Define functions to operate on the input fields
//--------------------------------------------------------
//------------------------------------------------------
// Step 7: Define output fields produced by the function
//------------------------------------------------------
//--------------------------------------
// Step 8: Create TridentTopology object
//--------------------------------------
//-------------------------------------------------
TridentTopology topology = new TridentTopology();
// Step 9: Create stream of batches using the spout
//-------------------------------------------------
//---------------------------------
Stream stream = topology.newStream("spout", spout);
Stream streamBatchAggregation = topology.newStream("batch-aggregation", spout);
Stream streamPersistentAggregation = topology.newStream("persistent-aggregation", spout);
// Step 10: Define what to do with each stream’s batch
//--------------------------------
//----------------------------------

streamBatchAggregation.groupBy(new Fields("actor")).aggregate(new Count(),new Fields("count")).each(new Fields("actor", "count"),new PrintFilter());

//streamPersistentAggregation.groupBy(new Fields("actor")).persistentAggregate(new MemoryMapState.Factory(),new Count(),new Fields("count")).each(new Fields("actor", "count"),new PrintFilter()).newValuesStream();
  // Use newValuesStream() to allow for further process of
    //   aggregated results, while persistent aggregation proceeds
streamPersistentAggregation
.groupBy(new Fields("actor"))
.persistentAggregate(new MemoryMapState.Factory(),
new Count(),new Fields("count"))
.newValuesStream()
.each(new Fields("actor", "count"), new PrintFilter())
;

stream.each(new Fields("actor"), new RegexFilter("doug")).each(new Fields("actor"), new ToUpperCaseFunction(),
    new Fields("uppercased_actor")).each(new Fields("uppercased_actor", "text"),
    new PrintFilter());



 // Step 11: Return the built topology
//----------------------------------
return topology.build();
}

public static void main(String[] args) throws Exception {
  Config conf = new Config();
if (args != null && args.length > 0) { conf.setNumWorkers(3);
          StormSubmitter.submitTopologyWithProgressBar(args[0],
                 conf, buildTopology());
} else {
conf.setMaxTaskParallelism(3);
LocalCluster cluster = new LocalCluster();
cluster.submitTopology("skeleton", conf, buildTopology()); }
}

}
