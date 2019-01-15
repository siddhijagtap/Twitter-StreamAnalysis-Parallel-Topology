/**
 * 
 */
package TWITTER_PRL;


public class BucketpacketInfo {
	
	private String hashTag = "";
	private int frequency = 0;
	private int maxError = 0;
	
	public BucketpacketInfo (String hashTag, int frequency, int maxError) {
		this.hashTag = hashTag;
		this.frequency = frequency;
		this.maxError = maxError;
	}
	
	public void SetHashTag(String tag) {
		this.hashTag = tag; 
	}
	
	public void setFrequency(int freq) {
		this.frequency = freq;
	}
	
	public void setMaxError(int err) {
		this.maxError = err;
	}
	
	public String getHashTag() {
		return this.hashTag;
	}
	
	public int getFrequency() {
		return this.frequency;
	}
	
	public int getMaxError() {
		return this.maxError;
	}
}
