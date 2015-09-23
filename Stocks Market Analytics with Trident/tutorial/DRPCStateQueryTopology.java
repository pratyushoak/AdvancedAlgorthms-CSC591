package storm.starter.trident.tutorial;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import com.google.common.collect.ImmutableList;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.FeederBatchSpout;
import storm.trident.testing.MemoryMapState;
import storm.starter.trident.tutorial.spouts.FakeTweetsBatchSpout;
import storm.starter.trident.tutorial.functions.SplitFunction;
//import storm.starter.trident.tutorial.functions.DivideFunction;
import storm.starter.trident.tutorial.filters.PrintFilter;
import storm.starter.trident.tutorial.filters.RegexFilter; 
import java.io.IOException;

public class DRPCStateQueryTopology {
	private static final String DATA_PATH = "data/500_sentences_en.txt";
  public static void main(String[] args)
throws Exception{
      Config conf = new Config();
      LocalCluster cluster =
          new LocalCluster();
      LocalDRPC drpc = new LocalDRPC();
      cluster.submitTopology("state_drpc", conf, buildTopology(drpc));
// The 1st arg in drpc.execute(): function name == first arg in newDRPCStrem()
// The 2nd arg: field tagged as "args" that needs to be parsed
for (int i = 0; i < 10; i++) { System.out.println("DRPC RESULT: " +
   drpc.execute("count_per_actors_event", "dave dave nathan"));
        Thread.sleep(1000);
}
}
private static StormTopology buildTopology(LocalDRPC drpc) throws IOException {
      TridentTopology topology = new TridentTopology();
      FakeTweetsBatchSpout spoutFakeTweets = new FakeTweetsBatchSpout(DATA_PATH);
      // Build a (key,value) persistent in-memory hash map: countState
// from the fake tweets spout
      TridentState countStateDBMS =
           topology
                .newStream("spout", spoutFakeTweets)
                .groupBy(new Fields("actor"))
                .persistentAggregate(new MemoryMapState.Factory(),
                 new Count(), new Fields("count"))
                ;
      // Query countStateDBMS hash by keys extracted from input arg string
      // and aggregate across multiple batches for each actor
            topology
                .newDRPCStream("count_per_actors_event", drpc)
                .each(new Fields("args"), new SplitFunction(" "),
                 new Fields("splitActor"))
                .stateQuery(countStateDBMS, new Fields("splitActor"),
                 new MapGet(), new Fields("per_actor_count"))
                .each(new Fields("splitActor", "per_actor_count"),
                 new FilterNull())
                .groupBy(new Fields("splitActor"))
                .aggregate(new Fields("per_actor_count"),
                 new Sum(), new Fields("sum"))
                ;
return topology.build(); }
}