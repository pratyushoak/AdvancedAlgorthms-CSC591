package storm.starter.trident.tutorial;
   import backtype.storm.Config;
   import backtype.storm.LocalCluster;
   import backtype.storm.LocalDRPC;
   import backtype.storm.generated.StormTopology;
   import backtype.storm.tuple.Fields;
   import storm.trident.TridentState;
   import storm.trident.TridentTopology;
   import storm.trident.operation.builtin.FilterNull;
   import storm.trident.operation.builtin.MapGet;
   import storm.trident.operation.builtin.Sum;
   import storm.trident.state.StateFactory;
   import storm.trident.testing.MemoryMapState;
   import storm.trident.testing.FeederBatchSpout;
   import storm.trident.testing.Split;
import storm.starter.trident.tutorial.spouts.CSVBatchSpout;
import storm.starter.trident.tutorial.functions.SplitFunction;
//import storm.starter.trident.tutorial.functions.DivideFunction;




public class DRPCStockQueryTopology { // Path relative to pom.xml file
       //path the the stocks file
       private static final String DATA_PATH = "data/stocks.csv.gz";
       /*************
        * Goals: (1) Start a Trident topology that reads stock symbols
        * and prices from a CSV data file. (2) Query the topology
        * with DRPC every 5 seconds.
        *************/

public static void main( String[] args ) throws Exception 
{ 
    Config conf = new Config();
    conf.setDebug(true);
    conf.setMaxSpoutPending( 20 );
           // This topology can only be run as local
           // TO DO: Create and submit a DRPC local cluster topology
           // Your goes here
    //Create local cluster 
    LocalCluster cluster = new LocalCluster();
    //Create local DRPC and submit topology
    LocalDRPC drpc = new LocalDRPC();
    cluster.submitTopology("stocks_drpc", conf, buildTopology(drpc));

for (int i = 0; i < 10; i++) 
      {
 
        /*  Print result of DRPC call with "trade_events" handler and arguments "AAPL, GE, INTC"
        */
        System.out.println("DRPC RESULT: " + drpc.execute("trade_events", "AAPL GE INTC"));
        
        Thread.sleep( 5000 ); //Sleep for 5 seconds between calls
      }

}
/*Class to build the topology 
*/
public static StormTopology buildTopology( LocalDRPC drpc ) 
{
             TridentTopology topology = new TridentTopology();

             // create a Fields object with the given fields
             Fields inputFields = new Fields("date", "symbol", "price", "shares");

             // Create a new spout from the file path and the fields object
             CSVBatchSpout spoutCSV = new CSVBatchSpout(DATA_PATH,inputFields); 

          // TO DO: Create TridentState in-memory hash map,
          //       called tradeVolumeDBMS
          //    It should ingest the data stream from the spoutCSV spout
          //    Group the tuples by "symbol" field
          //    Perform persistent aggregation and store (key,value)
          //       in in-process memory map
          //       where key is "shares" and "value"
          //       is aggregated field called "volume"
          // NOTE: groupBy() creates a "grouped stream"
          //       which means that subsequent aggregators
          //       will only affect Tuples within a group.
          //       groupBy() must always be followed by an aggregator.
          // NOTE: You can debug output of what spout emits with
          //      .each( new Fields( "date", "symbol",
          //             "price", "shares" ), new Debug() )
          // Your code goes here
     
     // Create a TridentState hashmap
     TridentState tradeVolumeDBMS =
           topology
                .newStream("spout", spoutCSV)       //new incoming data stream from spoutCSV
                .groupBy(new Fields("symbol"))      // group data by symbol field
                // persistent aggregation to store (key, value) pairs of (shares,volume)
                .persistentAggregate(new MemoryMapState.Factory(),  
                new Fields("shares"),
                new Sum(), new Fields("volume"))  //volume is sum of shares for each symbol
                ;
            /**
           * TO DO: Now setup a another stream--DRPC stream.
           * The DRPC stream should generate a list of "symbols"
           *    passed as args by the client.
           *    It should query the persistent aggregate state,
            * tradeVolumeDBMS, by "symbol" for
* the "volume" value.
* It should finally project() only
* the following fields of interest:
* "symbol" and "volume" that
* " DRPC client should print on stdout.
* NOTE: For debugging of args stmt use:
* .each( new Fields( "args" ), new Debug() )
* For debugging:
* .each( new Fields( "symbol" ), new Debug() )
* Debug print symbol and volume values:
* .each( new Fields( "symbol", "volume" ),
* new Debug() )
* To remove nulls for ’each’ value in the stream use
* .each( new Fields( "volume" ),
* new FilterNull() )
*****/
// Your code goes here
        topology
                .newDRPCStream("trade_events", drpc)  // create new DRPC stream 
                .each(new Fields("args"), new SplitFunction(" "),
                 new Fields("splitSymbol"))   // split input args to get symbols
                .stateQuery(tradeVolumeDBMS, new Fields("splitSymbol"), // query previously created hash-map for generated symbols
                 new MapGet(), new Fields("per_symbol_volume"))
                .each(new Fields("per_symbol_volume"),
                 new FilterNull())
                .groupBy(new Fields("splitSymbol")) //group by symbols
                .aggregate(new Fields("per_symbol_volume"), new Sum(), new Fields("volume"))
                .project(new Fields("splitSymbol","volume"))
                ;


//Return the built topology
return topology.build();
}
}