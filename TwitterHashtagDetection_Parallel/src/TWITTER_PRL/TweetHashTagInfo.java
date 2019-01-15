/**
 * 
 */
package TWITTER_PRL;


public class TweetHashTagInfo {
	
	private long createdTime;
	private String hashTag;
	
	public TweetHashTagInfo() {
	}
	
	public void setHashTagCreatedTimeStamp(long time) {
		this.createdTime = time;
	}
	
	public void setHashTag(String tag) {
		this.hashTag = tag;
	}
	
	public long getCreatedTimeStamp() {
		return this.createdTime;
	}
	
	public String getHashTag() {
		return this.hashTag;
	}
}
