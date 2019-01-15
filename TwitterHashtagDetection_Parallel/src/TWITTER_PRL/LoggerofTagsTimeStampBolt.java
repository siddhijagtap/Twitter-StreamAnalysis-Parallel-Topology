/**
 * 
 */
package TWITTER_PRL;

import java.io.File;
import java.io.PrintWriter;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import twitter4j.HashtagEntity;
import twitter4j.Status;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;


public class LoggerofTagsTimeStampBolt extends BaseRichBolt{

	private Map<Long,Set<String>> _timeStampTweetTagMap = null;
	private long _keeptrackTime;
	private PrintWriter _fw;
	
	@Override
	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub
//		Object oo = tuple.getValueByField("tweet-sentence");
//		Status status = (Status) oo;
		
//    		toWrite += "<"+hstg.getText()+">";
		String crtdtime = tuple.getStringByField("createdTime");
		String hashTag = tuple.getStringByField("HashTag");
		long crnttime = new Date().getTime()/1000;
		long crtdTime = Long.parseLong(crtdtime);
		
		if (_timeStampTweetTagMap.containsKey(crtdTime)) {
			
			Set<String> prevtags = _timeStampTweetTagMap.get(crtdTime);
			prevtags.add("<"+hashTag+">");
			_timeStampTweetTagMap.put(crtdTime, prevtags);
			
		} else {
			Set<String> tags = new HashSet<String>();
			tags.add("<"+hashTag+">");
			_timeStampTweetTagMap.put(crtdTime, tags);
		}
    	
		if ((crnttime - _keeptrackTime) >= 6) {
	    	for(long time : _timeStampTweetTagMap.keySet()) {
	    		String toWrite = "<"+String.valueOf(time)+"> :::: ";
	    		for(String tg : _timeStampTweetTagMap.get(time)) {
	    			toWrite += tg;
	    		}
	    		_fw.write(toWrite+"\n");
	    		_fw.flush();
	    		System.out.println("TWEET :::: "+toWrite);
	    		_timeStampTweetTagMap.remove(time);
	    	}
	    	_keeptrackTime = crnttime;
		}
	}

	@Override
	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		_timeStampTweetTagMap = new ConcurrentHashMap<Long, Set<String>>();
		_keeptrackTime = new Date().getTime()/1000;
		try{
			_fw = new PrintWriter(new File("/s/chopin/k/grad/amchakra/TweetlogParallel.txt"));
		}catch (Exception e) {
			System.out.println("UNABLE TO WRITE FILE :: 1 ");
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		
	}
	
	public void cleanup() {  
		_fw.close();
	}
}
