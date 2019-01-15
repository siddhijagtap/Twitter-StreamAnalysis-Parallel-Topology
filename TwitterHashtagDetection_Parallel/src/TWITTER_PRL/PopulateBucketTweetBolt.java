/**
 * 
 */
package TWITTER_PRL;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import twitter4j.HashtagEntity;
import twitter4j.Status;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class PopulateBucketTweetBolt extends BaseRichBolt{

	private int _bucketID = 0;
	private OutputCollector _collector = null;
	private Map<String,BucketpacketInfo> _Dbucket = null; 
	
	private int _bucktthresholdcnt = 0;
	private List<Object> _tweets = null; 
	private List<BucketpacketInfo> _perBucketproperTags = null;
	private int _bucketSize = 0;
	
	private long _trackTime;
	private double _s = 0.0; // 0.02;
	private double _epsilon = 0.0; //0.64;
	private int _N = 0;
	private int _W = 0;
	private int _typeFlag = 0;
	
	public PopulateBucketTweetBolt(int type_flg) {
		_typeFlag = type_flg;
	}
	
	@Override
	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub		
		String hashTag = tuple.getStringByField("HashTag");
		
		_collector.ack(tuple);
		
		long crntTime = new Date().getTime()/1000;
				
		if(!hashTag.isEmpty()) {
			if(_Dbucket.containsKey(hashTag)) { // Increase Frequency
				
				BucketpacketInfo prevInfo = _Dbucket.get(hashTag);
				prevInfo.setFrequency(prevInfo.getFrequency()+1); // Updated Frequency
				_Dbucket.put(hashTag, prevInfo); // overwriting again
				
				
			} else { // Just Push Info with proper values
				BucketpacketInfo info = new BucketpacketInfo(hashTag,1,(_bucketID-1));
				_Dbucket.put(hashTag, info);
			}
			
			_bucktthresholdcnt++;
			_N++;
		}
		
		if(_N%_W == 0) { // Reached Threshold : Bucket Size : Width //  _bucktthresholdcnt%_bucketSize
			
			for(String hshtg : _Dbucket.keySet()) {
				BucketpacketInfo bcktInfo = _Dbucket.get(hshtg);
				
				if(bcktInfo.getFrequency()+bcktInfo.getMaxError() > _bucketID) { // Keep Them and Emit them to DumpLogger
					System.out.println("KEPPING in D :: "+bcktInfo.getHashTag()+" : "+bcktInfo.getFrequency()+" : "+bcktInfo.getMaxError());
					if (_typeFlag == 1) {
						_collector.emit(new Values(bcktInfo.getHashTag(), bcktInfo.getFrequency(), (bcktInfo.getFrequency()+bcktInfo.getMaxError())));
					}
				} else { // Delete Them from Bucket  
					_Dbucket.remove(hshtg);
				}
			}
			
			_bucketID++;
		}
		if (_typeFlag == 0) {
			if ((crntTime - _trackTime) >= 6) { // EMIT NOW TO DUMP LOGGER 
				for(String hshtg : _Dbucket.keySet()) {
					BucketpacketInfo bcktInfo = _Dbucket.get(hshtg);
					System.out.println("BUCKET FRQ : "+bcktInfo.getFrequency()+" : "+_N+" : "+((_s-_epsilon)*_N)+" : "+(int)((_s-_epsilon)*_N)+" : "+_W);
					if (bcktInfo.getFrequency() >= ((_s-_epsilon)*_N)) {
						_collector.emit(new Values(bcktInfo.getHashTag(), bcktInfo.getFrequency(), (bcktInfo.getFrequency()+bcktInfo.getMaxError())));
					}
				}
				_trackTime = crntTime;
			}
		}
	}

	@Override
	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		
		_bucketID = 1;
		_bucketSize = 50;
		_bucktthresholdcnt = 0;
		_collector = collector;
		_tweets = new ArrayList<Object>();
		_Dbucket = new ConcurrentHashMap<String, BucketpacketInfo>();	
		_perBucketproperTags = new ArrayList<BucketpacketInfo>();
		
		_N = 0;
		_s = 0.01;
		_epsilon = (_typeFlag == 0) ? 0.004 : 0.02;
		_W = (int)(1/_epsilon);
		
		_trackTime = new Date().getTime()/1000;
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		

		declarer.declare(new Fields("tag","count","actual-count"));
		
	}
	


}
