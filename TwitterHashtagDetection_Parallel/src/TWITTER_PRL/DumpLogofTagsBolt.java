/**
 * 
 */
package TWITTER_PRL;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;


public class DumpLogofTagsBolt extends BaseRichBolt{
	
//	List<BucketpacketInfo> bucketHashTags = null;
	private Map<String, Integer> _calculatedcount = null;
	private Map<String, Integer> _actualcount = null;
	private Map<Long,Set<String>> _timeStampTagMap = null;
	private Map<Integer,Set<String>> _rankTagMap = null;
	private long _keeptrackTime;
	private PrintWriter _fw;
	private int _typefFlag;
	
	DumpLogofTagsBolt(int flag) {
		_typefFlag = flag;
	}
	@Override
	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub
		
//		Object oo = tuple.getValueByField("proper-bucket");
//		bucketHashTags = (List<BucketpacketInfo>) oo;
		
		System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$ IN EXECUTE OF DUMP LOG BOLT $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
		String tag = tuple.getStringByField("tag");
		int calcCount = tuple.getIntegerByField("count");
		int actlCount = tuple.getIntegerByField("actual-count");
		
		long time = new Date().getTime()/1000;
		
		if(_timeStampTagMap.containsKey(time)) {
			
			Set<String> prevtgs = _timeStampTagMap.get(time);
			prevtgs.add(tag);
			_timeStampTagMap.put(time, prevtgs);
			
		} else {
			Set<String> tgs = new HashSet<String>();
			tgs.add(tag);
			_timeStampTagMap.put(time, tgs);
		}
		
		
		_calculatedcount.put(tag, calcCount);
		_actualcount.put(tag, actlCount);
		
		
		System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$ IN EXECUTE OF DUMP LOG BOLT :: "+String.valueOf(time)+":: "+String.valueOf(_keeptrackTime)+"$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
		if ((time - _keeptrackTime) >= 10) {
			
			int timeDiff = (int) (time - _keeptrackTime);
			int div = timeDiff/10;
			
			System.out.println(timeDiff+" ---- "+div);
			
			List<Long> times = new ArrayList<Long>(_timeStampTagMap.keySet());
			
			for (int i = 0; i<div; i++) {
				long strttime = _keeptrackTime + (long)(i*10);
				long endtime = _keeptrackTime + (long)((i+1)*10-1);
				
				System.out.println(strttime+" *----* "+endtime);
				System.out.println("*****************************  LOG CREATED from "+String.valueOf(strttime)+" to "+String.valueOf(endtime)+"  **********************************************");
				_fw.write("*****************************  LOG CREATED from "+String.valueOf(strttime)+" to "+String.valueOf(endtime)+"  **********************************************");
				_fw.write(System.lineSeparator());
				
				for (long tm : times) {
					if ((tm >= strttime) && (tm <= endtime)) {
						DumpininMemoryMap(strttime, endtime, _timeStampTagMap.get(tm));
						_timeStampTagMap.remove(tm);
					}
				}
				ShowInLogFile();
				System.out.println("********************************************************************************************************************************");
				_fw.write("************************************************************************************************************************************************************");
				_fw.write(System.lineSeparator());
			}
			
			
//			if((_calculatedcount.size() != 0) && (_actualcount.size() != 0)) {
//				DumpInFile(1);
//			} else {
//				DumpInFile(0);
//			}
			
			_keeptrackTime = _keeptrackTime+(long)(div*10); // new Date().getTime()/1000;
//			_calculatedcount.clear();
//			_actualcount.clear();
		} 
	}
	
	private void ShowInLogFile() {
		CopyOnWriteArrayList<Integer> Secs10ranks = new CopyOnWriteArrayList<Integer>(_rankTagMap.keySet());
		Collections.sort(Secs10ranks, Collections.reverseOrder());
		CopyOnWriteArrayList<Integer> top100Ranks = null;
		if(Secs10ranks.size() >= 100) {
			top100Ranks = new CopyOnWriteArrayList<Integer>(Secs10ranks.subList(0,100));
		} else {
			top100Ranks = new CopyOnWriteArrayList<Integer>(Secs10ranks.subList(0,Secs10ranks.size()));
		}
		
		int i = 0;
		for (int rnk : top100Ranks) {
			for (String hshtg : _rankTagMap.get(rnk)) {
				System.out.println(String.valueOf(rnk)+" SORTED TAGS ::  TAG : "+hshtg+" : COUNT : "+_calculatedcount.get(hshtg)+" : ACTUAL : "+_actualcount.get(hshtg));
				try {
					_fw.write("INFO TAGS :: TAG : "+hshtg+" : COUNT : "+_calculatedcount.get(hshtg)+" : ACTUAL : "+_actualcount.get(hshtg));
					_fw.write(System.lineSeparator());
					_fw.flush();
				} catch (Exception e) {
					System.out.println("PROBLEM FILE WRITE :::: ");
					e.printStackTrace();
				}
//				Secs10ranks.remove(Secs10ranks.get(rnk));
//				top100Ranks.remove(top100Ranks.get(rnk));
				_calculatedcount.remove(hshtg);
				_actualcount.remove(hshtg);
			}
			i++;
		}
		_rankTagMap.clear();
		Secs10ranks.clear();
		top100Ranks.clear();
	}
	
	private void DumpininMemoryMap(long start, long end, Set<String> hashTags) {
		for(String hshtg : hashTags) {
			if((_calculatedcount.containsKey(hshtg))) {
				if((_calculatedcount.get(hshtg) != null) && (_actualcount.get(hshtg) != null)) {
//					System.out.println(" TAG : "+hshtg+" : COUNT : "+_calculatedcount.get(hshtg)+" : ACTUAL : "+_actualcount.get(hshtg));
					int rank = _calculatedcount.get(hshtg);
					if(_rankTagMap.containsKey(rank)) {
						
						Set<String> prevtgs = _rankTagMap.get(rank);
						prevtgs.add(hshtg);
						_rankTagMap.put(rank, prevtgs);
						
					} else {
						
						Set<String> tgs = new HashSet<String>();
						tgs.add(hshtg);
						_rankTagMap.put(rank, tgs);
						
					}
				}
			}
		}
		
	}
	
	private void DumpInFile(int flag) {
		try {
			System.out.println("****************** FLAG :: "+String.valueOf(flag)+" *****************************************");
			System.out.println("***************************** LOG CREATED at "+String.valueOf(_keeptrackTime)+"**********************************************");
//			_fw = new FileWriter(new File("/s/chopin/k/grad/amchakra/CS535_PA2/"+String.valueOf(_keeptrackTime)+".txt"));
			_fw.write("***************************** LOG CREATED at "+String.valueOf(_keeptrackTime)+"**********************************************\n");
			if(flag == 1) {
				for (String tag : _calculatedcount.keySet()) {
					_fw.write(" TAG : "+tag+" : COUNT : "+_calculatedcount.get(tag)+" : ACTUAL : "+_actualcount.get(tag)+"\n");
	//				_fw.write(System.lineSeparator());
					System.out.println(" TAG : "+tag+" : COUNT : "+_calculatedcount.get(tag)+" : ACTUAL : "+_actualcount.get(tag));
				}
			} else {
				_fw.write(" %%%%%%%%%%%%%%%%%%%% NO LOG CREATED FOR THIS 10 SECONDS %%%%%%%%%%%%%%%%%%%%%%%%%%%%");
			}
	//		for(BucketpacketInfo info : bucketHashTags) {
	//			System.out.println(" HASTAG : "+info.getHashTag()+" : FREQUENCY : "+String.valueOf(info.getFrequency())+" : ACTUAL FREQUENCY : "+String.valueOf(info.getFrequency()+info.getMaxError()));
	//		}
			_fw.write("*****************************************************************************************************************************************\n");
			System.out.println("********************************************************************************************************************************");
		} catch (Exception e) {
			System.out.println("UNABLE TO WRITE FILE :: 2 ");
			e.printStackTrace();
		}
	}
	
	@Override
	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
//		bucketHashTags = new ArrayList<BucketpacketInfo>();
		
		System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$ IN PREPARE OF DUMP LOG BOLT $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
		_calculatedcount = new ConcurrentHashMap<String, Integer>();
		_actualcount = new ConcurrentHashMap<String, Integer>();
		_keeptrackTime = new Date().getTime()/1000;
		_timeStampTagMap = new ConcurrentHashMap<Long, Set<String>>();
		_rankTagMap = new ConcurrentHashMap<Integer, Set<String>>();
		
		
		try{
			if(_typefFlag == 1) {
				_fw = new PrintWriter(new File("/s/chopin/k/grad/amchakra/TweetLossyCountBoltWithout_S.txt"));
			} else {
				_fw = new PrintWriter(new File("/s/chopin/k/grad/amchakra/TweetLossyCountBoltWith_S.txt"));
			}
		}catch (Exception e) {
			System.out.println("UNABLE TO WRITE FILE :: 1 ");
			e.printStackTrace();
		}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}
	

}
