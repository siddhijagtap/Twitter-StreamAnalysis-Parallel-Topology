package TWITTER_PRL;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;


public class Twittertopologyprl {
	
	private static final String READSTREAM_SPOUT_ID = "stream-spout";
	private static final String POPULATEBUCKET_BOLT_ID = "populate-bolt";
	private static final String TWEETLOGOUTPUT_BOLT_ID = "tweet-log-bolt";
	private static final String LOGOUTPUT_BOLT_ID = "log-bolt";
	private static final String TOPOLOGY_NAME = "read-stream-topology";
	
	public static void main( String[] args) throws Exception { 
		
		int type_flag = Integer.parseInt(args[1]);
		
		ReadStreamSpout spout = new ReadStreamSpout();
		
		PopulateBucketTweetBolt populatebolt = new PopulateBucketTweetBolt(type_flag);
		DumpLogofTagsBolt logbolt = new DumpLogofTagsBolt(type_flag);
		LoggerofTagsTimeStampBolt twtLogBolt = new LoggerofTagsTimeStampBolt();
		
		TopologyBuilder builder = new TopologyBuilder(); 
		
		
		builder.setSpout(READSTREAM_SPOUT_ID, spout);
		
		if(args.length > 0 ) {
			if (args[0].equals("REMOTE-PARALLEL")) {
				builder.setBolt(POPULATEBUCKET_BOLT_ID, populatebolt, 4)
//										.setNumTasks(4)
										.fieldsGrouping(READSTREAM_SPOUT_ID, new Fields("HashTag")); // shuffleGrouping(READSTREAM_SPOUT_ID); 
				
				builder.setBolt(TWEETLOGOUTPUT_BOLT_ID, twtLogBolt)
					.globalGrouping(READSTREAM_SPOUT_ID); 

				builder.setBolt(LOGOUTPUT_BOLT_ID, logbolt)
					.globalGrouping(POPULATEBUCKET_BOLT_ID);
				
			} else if (args[0].equals("REMOTE")) {
				
				builder.setBolt(POPULATEBUCKET_BOLT_ID, populatebolt)
										.shuffleGrouping(READSTREAM_SPOUT_ID); 
				
				builder.setBolt(TWEETLOGOUTPUT_BOLT_ID, twtLogBolt)
					.globalGrouping(READSTREAM_SPOUT_ID); 

				builder.setBolt(LOGOUTPUT_BOLT_ID, logbolt)
					.fieldsGrouping(POPULATEBUCKET_BOLT_ID, new Fields("tag"));
				
			} else if (args[0].equals("LOCAL")) {
				
				builder.setBolt(POPULATEBUCKET_BOLT_ID, populatebolt)
					.shuffleGrouping(READSTREAM_SPOUT_ID); 
				
				builder.setBolt(TWEETLOGOUTPUT_BOLT_ID, twtLogBolt)
					.globalGrouping(READSTREAM_SPOUT_ID); 

				builder.setBolt(LOGOUTPUT_BOLT_ID, logbolt)
					.fieldsGrouping(POPULATEBUCKET_BOLT_ID, new Fields("tag"));
			}
		} 
		
		/*builder.setBolt(TWEETLOGOUTPUT_BOLT_ID, twtLogBolt)
										.globalGrouping(READSTREAM_SPOUT_ID); 
		
		builder.setBolt(LOGOUTPUT_BOLT_ID, logbolt)
										.fieldsGrouping(POPULATEBUCKET_BOLT_ID, new Fields("tag"));*/ 
		
		Config config = new Config();
		config.setDebug(false); // true
		config.put(config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		
		if(args.length > 0 ) {
			
			if (args[0].contains("REMOTE")) {
			
				config.setNumWorkers(4);
				StormSubmitter.submitTopologyWithProgressBar(args[0], config, builder.createTopology());
			
			} else {
				  
				LocalCluster cluster = new LocalCluster(); 
				cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology()); 
				
				try {
					System.out.println("######################### ENTERING INTO SLEEP ##################################");
					
					Thread.sleep(1000000);
					
				} catch (Exception e) {
					System.out.println("######################### GETTING OUT OF SLEEP ##################################");
					e.printStackTrace();
				}
				
				cluster.killTopology(TOPOLOGY_NAME); 
				cluster.shutdown();
			}
		} 
	}
}




