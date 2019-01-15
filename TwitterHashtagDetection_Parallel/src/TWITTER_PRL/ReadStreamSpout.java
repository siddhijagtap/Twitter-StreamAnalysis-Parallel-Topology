/**
 * 
 */
package TWITTER_PRL;

import java.io.File;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import twitter4j.HashtagEntity;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;



public class ReadStreamSpout extends BaseRichSpout{
	
	private SpoutOutputCollector _collector; 
	private TweetHashTagInfo _twthashtgInfo;
	private LinkedBlockingQueue<TweetHashTagInfo> _msgs;
//	private Map<Long,Set<String>> _timeStampTweetTagMap = null;
	private TwitterStream _stream;
//	private PrintWriter _fw;
	
    
	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		TweetHashTagInfo s = _msgs.poll();
		
        if (s == null) {
//            Utils.sleep(1000);
        	try{
        		Thread.sleep(100);
        	} catch(Exception e){
        		e.printStackTrace();
        	}
        } else {
            _collector.emit(new Values(String.valueOf(s.getCreatedTimeStamp()), s.getHashTag()));
//            System.out.println("************** IN SPOUT ***************** :: "+s);

        }
		
	}

	@Override
	public void open(Map config, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		
		_msgs = new LinkedBlockingQueue<TweetHashTagInfo>();
		_twthashtgInfo = new TweetHashTagInfo();
		this._collector = collector; 
		

		
		StatusListener listener = new StatusListener(){
			@Override
			public void onStatus(Status status) {
//	            System.out.println("!!!!!!!!!! ---> "+status.getUser().getName() + " : " + status.getText());
//	        	_msgs.offer(status); 
				for (HashtagEntity hstg : status.getHashtagEntities()) {
					_twthashtgInfo.setHashTagCreatedTimeStamp(status.getCreatedAt().getTime());
					_twthashtgInfo.setHashTag(hstg.getText().toLowerCase());
					_msgs.offer(_twthashtgInfo);
				}
	        	
	        }
	        
			@Override
			public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
//				System.out.println("************* IN DELETION NOTICE ***********************");
			}
	        
			@Override
	        public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
//				System.out.println("************* IN TRACk LIMITATION NOTICE ***********************");
			}
	        
			@Override
	        public void onException(Exception ex) {
	            ex.printStackTrace();
	        }
			
			@Override
			public void onScrubGeo(long arg0, long arg1) {
				// TODO Auto-generated method stub
				
			}
			@Override
			public void onStallWarning(StallWarning arg0) {
				// TODO Auto-generated method stub
				
			}
		};
		
		ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true);
        cb.setJSONStoreEnabled(true);
        
        cb.setOAuthConsumerKey("rm0NYAl2RoDFEV4AkqiR9AjNZ");
        cb.setOAuthConsumerSecret("TEOqkyZDBMRW3cQLWEmwX9BXBeUnq35rLxpaNrvaQhoGv6Kwbi");
        cb.setOAuthAccessToken("4130873239-aDi90Ihhd4kVUNgNfg7kxRuZNPal8URcyXfCHEw");
        cb.setOAuthAccessTokenSecret("VB1qdjU4W6E6fDfP0swLaMBIhNcDoB34sOq3SQTKe5VvS");
        
		TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
		
		twitterStream.addListener(listener);
		twitterStream.sample();
        System.out.println("Connect Successful");
        

		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("createdTime","HashTag"));
		
	}
	
	

}
