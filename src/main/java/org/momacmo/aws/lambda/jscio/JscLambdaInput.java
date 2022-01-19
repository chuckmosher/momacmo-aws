package org.momacmo.aws.lambda.jscio;

import java.util.Map;

public class JscLambdaInput {
	public String bucket;
	public String prefix;
	public int volume;
	public int frame;
	public int nsamp;
	public int ntrace;
	public int nhdr;
	
	public JscLambdaInput() {
	  this.bucket = "bucket";
	  this.prefix = "prefix";
	  this.volume = 0;
	  this.frame = 0;
	}
	
	public JscLambdaInput(Map<String,String> eventMap) {
	  bucket = eventMap.get("bucket");
	  prefix = eventMap.get("prefix");
    nsamp = Integer.parseInt(eventMap.get("nsamp"));
    ntrace = Integer.parseInt(eventMap.get("ntrace"));
	  volume = Integer.parseInt(eventMap.get("volume"));
	  frame = Integer.parseInt(eventMap.get("frame"));
	}
	
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
  public int getNsamp() {
    return nsamp;
  }
  public void setNsamp(int nsamp) {
    this.nsamp = nsamp;
  }
  public int getNtrace() {
    return ntrace;
  }
  public void setNtrace(int ntrace) {
    this.ntrace = ntrace;
  }
  public int getNhdr() {
    return nhdr;
  }
  public void setNhdr(int nhdr) {
    this.nhdr = nhdr;
  }
}

