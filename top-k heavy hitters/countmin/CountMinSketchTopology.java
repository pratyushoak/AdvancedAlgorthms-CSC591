package storm.starter.trident.project.countmin; 

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
import storm.trident.testing.Split;
import storm.trident.testing.FixedBatchSpout;
import backtype.storm.tuple.Values;
import storm.starter.trident.project.countmin.AddToBloomFilter;
import java.util.Arrays;

import storm.trident.operation.builtin.Count;
import storm.starter.trident.project.functions.ToLowerCaseFunction;
import storm.starter.trident.project.functions.SentenceBuilder;
import storm.starter.trident.project.functions.ParseTweet;

import storm.starter.trident.project.countmin.state.CountMinSketchStateFactory;
import storm.starter.trident.project.countmin.state.CountMinQuery;
import storm.starter.trident.project.countmin.state.CountMinSketchUpdater;
import storm.starter.trident.project.countmin.state.CountMin;
//import storm.starter.trident.project.functions.SplitFunction;
import storm.starter.trident.project.spouts.TwitterSampleSpout;

/**
 *@author: Preetham MS (pmahish@ncsu.edu)
 *@modified by: Pratyush Oak
 */


public class CountMinSketchTopology {

	 public static StormTopology buildTopology( String[] args, LocalDRPC drpc ) {

        TridentTopology topology = new TridentTopology();

        int width = 10;
		int depth = 15;
		int seed = 10;
		int k = 10;	
	
		// Twitter's account credentials passed as args
        String consumerKey = args[0];
        String consumerSecret = args[1];
        String accessToken = args[2];
        String accessTokenSecret = args[3];

        //Twitter topic of interest
        String[] arguments = args.clone();
        String[] topicWords = Arrays.copyOfRange(arguments, 4, arguments.length);
        
        // Create Twitter's spout
		TwitterSampleSpout spoutTweets = new TwitterSampleSpout(consumerKey, consumerSecret,
									accessToken, accessTokenSecret, topicWords);
/*
    	FixedBatchSpout spoutFixedBatch = new FixedBatchSpout(new Fields("sentence"), 3,
			new Values("the cow jumped over the moon"),
			new Values("the man went to the store and bought some candy"),
			new Values("four score and seven years ago"),
			new Values("how many apples can you eat"),
			new Values("to be or not to be the person"))
			;
		spoutFixedBatch.setCycle(false);*/

		//build topology to obtain persisten collection of words-countMinDBMS from input Twitter stream
		TridentState countMinDBMS= topology.newStream("tweets",spoutTweets)
		.each(new Fields("tweet"), new ParseTweet(), new Fields("text", "tweetId", "user"))
		.each(new Fields("text", "tweetId", "user"), new SentenceBuilder(), new Fields("sentence"))     	// Form the sentence with tweet text
		.each(new Fields("sentence"), new Split(), new Fields("words"))								// Split each tweet sentence into words (space-delimited)
		.each(new Fields("words"), new ToLowerCaseFunction(), new Fields("words_lower"))							// convert each of the word into lowercase
		.each(new Fields("words_lower"), new AddToBloomFilter())															// Pass each word with BloomFilter
		.partitionPersist( new CountMinSketchStateFactory(depth,width,seed,k), new Fields("words_lower"), new CountMinSketchUpdater())	// CountMinSketchStateFactory creates a count-min data structure for the filtered words
		;	

		//query countMinDBMS for top k words and diplay
		topology.newDRPCStream("get_count", drpc)
			.stateQuery(countMinDBMS, new Fields("args"), new CountMin(), new Fields("count"))
			.project(new Fields("args", "count"))
			;
		//build the topology
		return topology.build(); 

	}


	public static void main(String[] args) throws Exception {
		Config conf = new Config();
        	conf.setDebug( false );
        	conf.setMaxSpoutPending( 10 );

        	//create local cluster 
        	LocalCluster cluster = new LocalCluster();
        	//create local a DRPC
        	LocalDRPC drpc = new LocalDRPC();
        	//submit the get_count topology along with the drpc, config and the twitter credentials as the args
        	cluster.submitTopology("get_count",conf,buildTopology(args,drpc));

        	for (int i = 0; i < 5; i++) {
        			//execute drpc event passing a dummy argument till the topk are retrived
            		System.out.println("DRPC RESULT:"+ drpc.execute("get_count","abc"));
            		Thread.sleep( 10000 );
        	
}
		System.out.println("STATUS: OK");
		//cluster.shutdown();
        	//drpc.shutdown();
	}
}
