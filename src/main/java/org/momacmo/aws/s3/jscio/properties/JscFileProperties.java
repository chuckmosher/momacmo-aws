package org.momacmo.aws.s3.jscio.properties;

import java.nio.ByteOrder;
import java.time.Instant;

import org.javaseis.grid.BinGrid;
import org.javaseis.grid.GridDefinition;
import org.javaseis.properties.DataFormat;
import org.javaseis.properties.DataType;
import org.momacmo.aws.s3.jscio.JsAwsS3Parms;

public class JscFileProperties {
  public JsAwsS3Parms jscParms;
  public String version;
  public DataType dataType;
  public DataFormat traceFormat;
  public ByteOrder byteOrder;
  public boolean mapped = false;
  public GridDefinition gridDefinition;
  public boolean hasBinGrid = false;
  public BinGrid binGrid;
  public boolean usesTraceProperties = false;
  public TracePropertiesImpl traceProperties;
  public Instant timeZero;
}
