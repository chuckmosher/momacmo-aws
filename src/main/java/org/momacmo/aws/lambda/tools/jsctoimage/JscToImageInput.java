package org.momacmo.aws.lambda.tools.jsctoimage;

import java.util.Map;

public class JscToImageInput {

  public String profile = "profile";
  public String region = "region";
  public String bucket = "bucket";
  public String prefix = "prefix";
  public int volume = 0;
  public int frame  = 0;
  public float scaleMin = -1;
  public float scaleMax = 1;
  public DisplayColorModel.ColorModel colorScale = DisplayColorModel.ColorModel.GRAY;
  
  public JscToImageInput() {
  }
  
  public JscToImageInput( String profile, String region, String bucket, String prefix,
      int frame, int volume, float scaleMin, float scaleMax, DisplayColorModel.ColorModel colorScale ) {
    this.profile = profile;
    this.region = region;
    this.bucket = bucket;
    this.prefix = prefix;
    this.volume = volume;
    this.frame = frame;
    this.scaleMin = scaleMin;
    this.scaleMax = scaleMax;
    this.colorScale = colorScale;
  }

  public JscToImageInput(Map<String, String> eventMap) { 
    if (eventMap.containsKey("profile"))
    profile = eventMap.get("profile");
    if (eventMap.containsKey("region"))
    region = eventMap.get("region");
    if (eventMap.containsKey("bucket"))
    bucket = eventMap.get("bucket");
    if (eventMap.containsKey("prefix"))
    prefix = eventMap.get("prefix");
    if (eventMap.containsKey("volume"))
    volume = Integer.parseInt(eventMap.get("volume"));
    if (eventMap.containsKey("frame"))
    frame = Integer.parseInt(eventMap.get("frame"));
    if (eventMap.containsKey("scaleMin"))
    scaleMin = Float.parseFloat(eventMap.get("scaleMin"));
    if (eventMap.containsKey("scaleMax"))
    scaleMax = Float.parseFloat(eventMap.get("scaleMax"));
    if (eventMap.containsKey("colorScale"))
    colorScale = DisplayColorModel.ColorModel.valueOf(eventMap.get("colorScale"));
  }

}
