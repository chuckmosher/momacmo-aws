package org.momacmo.aws.lambda.jscio;

public class JscLambdaInput {
	public String bucket;
	public String prefix;
	public int volume;
	public int frame;
	public String getBucket() {
		return bucket;
	}
	public void setBucket(String bucket) {
		this.bucket = bucket;
	}
	public String getPrefix() {
		return prefix;
	}
	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}
	public int getVolume() {
		return volume;
	}
	public void setVolume(int volume) {
		this.volume = volume;
	}
  public int getFrame() {
    return frame;
  }
  public void setFrame(int frame) {
    this.frame = frame;
  }
}

