package org.momacmo.aws.s3.jscio.properties;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.javaseis.properties.PropertyDescription;
import org.javaseis.properties.TraceProperties;
import org.javaseis.util.SeisException;

public class TracePropertiesImpl extends TraceProperties {
  private static final long serialVersionUID = 1L;
  private int origRecordLength = -1;

  public TracePropertiesImpl() {
    super();
  }
  
  public TracePropertiesImpl( TraceProperties tp ) {
    
  }

  public void setRecordLength(int newRecordLength) {
    if (newRecordLength < super._recordLength)
      throw new IllegalArgumentException(
          "New record length " + newRecordLength + " is shorter than existing length " + super._recordLength);
    origRecordLength = super._recordLength;
    super._recordLength = newRecordLength;
  }
  
  public int getHeaderLength() {
    if (origRecordLength < 0) return super._recordLength;
    return origRecordLength;
  }

}
