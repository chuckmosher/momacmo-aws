package org.momacmo.aws.s3.jscio;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.javaseis.compress.TraceCompressor;
import org.javaseis.grid.BinGrid;
import org.javaseis.grid.GridDefinition;
import org.javaseis.properties.TraceProperties;
import org.javaseis.util.SeisException;
import org.momacmo.aws.s3.jscio.properties.JscFileProperties;
import org.momacmo.aws.s3.jscio.properties.JsonUtil;
import org.momacmo.aws.s3.jscio.properties.TracePropertiesImpl;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;

/**
 * Example implementation of using AWS S3 Object Storage to hold traces and
 * headers for a SeisSpace JavaSeis dataset. Existing machinery and SeisSpace
 * code is used to manage metadata, grid and header definitions, and so on.
 * <p>
 * Current implementation requires a 4D framework
 * <p>
 * In this example we use an AWS Bucket and Prefix as the root directory for
 * metadata, traces, and headers. For example:
 * <p>
 * AWS Bucket = data<br>
 * AWS Prefix = Project/SubProject
 * <p>
 * We create 'folders' and store data under the Bucket/Prefix
 * <p>
 * <code>
 * data/Project/SubProject/FileProperties.xml
 * <br>
 * data/Project/SubProject/Traces/
 * <br>
 * data/Project/SubProject/Headers/
 * </code>
 * <p>
 * Traces and headers are stored frame by frame using a key suffix based on the
 * volume and frame index headers. For example, for Volume Index 11, Frame 134,
 * the traces and headers would be stored as:
 * <p>
 * <code>
 * data/Project/SubProject/Traces/V11/F134
 * <br>
 * data/Project/SubProject/Headers/V11/F134
 * </code>
 * 
 * @author Chuck Mosher for MoMacMo.org
 *
 */
public class JsAwsS3 {
  JscFileProperties jscFileProperties;
  public static String FILE_PROPERTIES_JSC = "JscFileProperties.json";
  // AWS bucket, prefix, and client
  String awsProfile, awsRegion;
  String awsBucket;
  String awsPrefix;
  AmazonS3 s3;
  // Runtime objects used internally
  ByteBuffer trcBuffer, hdrBuffer;
  TraceCompressor traceCompressor;
  int recordLength;
  int hdrLength, hdrWords;
  int nsamp, maxTraces;
  byte[] trcBytes, hdrBytes;
  IntBuffer intBuffer;
  // True if a dataset is open
  boolean isOpen;
  // Volume and frame range for the dataset
  int[] volRange, frmRange;
  int frameCount;
  int[] pos = new int[4];
  // Buffer for S3 transfers
  byte[] xfrBytes = new byte[16384];

  /**
   * Return true if this SeisSpace dataset uses AWS S3 to store traces and headers
   * 
   * @param jsPath - full path to the SeisSpace JavaSeis dataset directory
   * @return - true if this is dataset uses AWS S3 Object Storage
   */
  public static boolean isJsAwsS3(String jsPath) {
    File f = new File(jsPath);
    if (!f.isDirectory())
      return false;
    File fj = new File(jsPath + File.separator + FILE_PROPERTIES_JSC);
    if (!fj.exists())
      return false;
    return true;
  }
  
  public static boolean isJsAwsS3( JsAwsS3Parms parms) {
    return isJsAwsS3( parms.awsProfile, parms.awsRegion, parms.awsBucket, parms.awsPrefix );
  }

  /**
   * Return true if this SeisSpace dataset uses AWS S3 to store traces and headers
   * 
   * @param jsPath - full path to the SeisSpace JavaSeis dataset directory
   * @return - true if this is dataset uses AWS S3 Object Storage
   */
  public static boolean isJsAwsS3(String awsProfileName, String awsRegionName, String awsBucketName,
      String awsPrefixName) {
    AWSCredentials credentials = null;
    AmazonS3 s3Tmp = null;
    try {
      credentials = new ProfileCredentialsProvider(awsProfileName).getCredentials();
      if (awsRegionName == null || awsRegionName == "default") {
        s3Tmp = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials)).build();
      } else {
        s3Tmp = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials))
            .withRegion(awsRegionName).build();
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("Cannot load AWS credentials from the credential profiles file. "
          + "Please make sure that your credentials file is at the correct "
          + "location (~/.aws/credentials), and is in valid format.");
      return false;
    }
    if (s3Tmp.doesBucketExistV2(awsBucketName) == false) {
      s3Tmp.shutdown();
      return false;
    }
    if (s3Tmp.doesObjectExist(awsBucketName, awsPrefixName + "/" + FILE_PROPERTIES_JSC) == false) {
      s3Tmp.shutdown();
      return false;
    }
    s3Tmp.shutdown();
    return true;
  }

  /**
   * Return the GridDefition for a JavaSeis Cloud AWS S3 Dataset
   * 
   * @param awsProfileName - credentials profile
   * @param awsRegionName  - region name
   * @param awsBucketName  - bucket name
   * @param awsPrefixName  - prefix name
   * @return GridDefintion for the dataset
   * @throws SeisException on access errors
   */
  public static JscFileProperties getFileProperties(String awsProfileName, String awsRegionName, String awsBucketName,
      String awsPrefixName) throws SeisException {
    AWSCredentials credentials = null;
    AmazonS3 s3Tmp = null;
    String awsRegion = new String(awsRegionName);
    try {
      credentials = new ProfileCredentialsProvider(awsProfileName).getCredentials();
      if (awsRegionName == null || awsRegionName == "default") {
        s3Tmp = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials)).build();
        awsRegion = s3Tmp.getRegionName();
      } else {
        s3Tmp = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials))
            .withRegion(awsRegionName).build();
        awsRegion = awsRegionName;
      }
    } catch (Exception e) {
      throw new SeisException("Invalid AWS credentials for profile: " + awsProfileName);
    }
    if (s3Tmp.doesBucketExistV2(awsBucketName) == false) {
      s3Tmp.shutdown();
      throw new SeisException("AWS Region " + awsRegion + " does not contain bucket: " + awsBucketName);
    }
    String key = awsPrefixName + "/" + FILE_PROPERTIES_JSC;
    if (s3Tmp.doesObjectExist(awsBucketName, key) == false) {
      s3Tmp.shutdown();
      throw new SeisException(
          "AWS Region " + awsRegion + " Bucket " + awsBucketName + " does not contain object: " + key);
    }
    JscFileProperties fprops = (JscFileProperties) getJsonObject( s3Tmp, awsBucketName, key, JscFileProperties.class );    
    return fprops;
  }

  public static String getAwsPrefixFromPath(String jsDatasetPath) {
    if (jsDatasetPath == null)
      return "null";
    File f = new File(jsDatasetPath);
    String name = f.getName();
    if (name == null)
      return "null";
    if (name.endsWith(".js")) {
      name = name.substring(0, name.length() - 3);
    } else {
      return "null";
    }
    List<String> dirs = new ArrayList<String>();
    do {
      dirs.add(f.getName());
      f = f.getParentFile();
    } while (f.getParentFile() != null);
    if (dirs.size() < 2)
      return ("null");
    return dirs.get(2) + "/" + dirs.get(1) + "/" + name;
  }

  /** Close and release all resources */
  public void shutdown() {
    // Probably not necessary but release and nullify big objects
    s3.shutdown();
    s3 = null;
    awsBucket = null;
    awsPrefix = null;
    closeFile();
  }

  /** close the current file and associated resources */
  public void closeFile() {
    if (isOpen) {
      intBuffer = null;
      hdrBuffer = null;
      hdrBytes = null;
      trcBuffer = null;
      trcBytes = null;
      traceCompressor = null;
      isOpen = false;
    }
  }

  /**
   * Initialize JavaSeis AWS-S3 access using arguments from command line
   * 
   * @param args - String array containing: [0] - AWS Profile [1] = AWS Region [2]
   *             - AWS Bucket [3] - AWS Prefix
   * @throws SeisException - on access errors
   */
  public JsAwsS3(String[] args) throws SeisException {
    this(args[0], args[1]);
    openRemote(args[2], args[3]);
  }
  
  /**
   * Open a JavaSeis Cloud datset
   * @param awsProfile - AWS Credentials profile
   * @param awsRegion - AWS Region name
   * @param awsBucket - AWS Bucket name
   * @param awsPrefix - AWS Prefix for the "folder" containing the dataset
   * @throws SeisException - on AWS access and dataset errors
   */
  public JsAwsS3(String awsProfile, String awsRegion, String awsBucket, String awsPrefix) throws SeisException {
    this(awsProfile, awsRegion);
    openRemote(awsBucket, awsPrefix);
  }  
  
  /**
   * Open a JavaSeis Cloud datset
   * @param awsProfile - AWS Credentials profile
   * @param awsRegion - AWS Region name
   * @param awsBucket - AWS Bucket name
   * @param awsPrefix - AWS Prefix for the "folder" containing the dataset
   * @throws SeisException - on AWS access and dataset errors
   */
  public JsAwsS3(JsAwsS3Parms parms) throws SeisException {
    this(parms.awsProfile, parms.awsRegion);
    openRemote(parms.awsBucket, parms.awsPrefix);
  }

  /**
   * Initialize JavaSeis AWS-S3 access using an existing AWS Credentials Profile
   * 
   * @param awsProfile - AWS Credentials profile in the standard location
   * @throws SeisException - on access errors
   */
  public JsAwsS3(String awsProfile) throws SeisException {
    this(awsProfile, "default");
  }

  /**
   * Initialize JavaSeis AWS-S3 access using an existing AWS Credentials Profile
   * 
   * @param awsProfile - AWS Credentials profile in the standard location
   * @throws SeisException - on access errors
   */
  public JsAwsS3(String awsProfileName, String awsRegionName) throws SeisException {
    AWSCredentials credentials = null;
    awsProfile = awsProfileName;
    if (awsRegionName == null)
      awsRegionName = "default";
    awsRegion = awsRegionName;
    try {
      credentials = new ProfileCredentialsProvider(awsProfile).getCredentials();
      if (awsRegion.equals("default")) {
        s3 = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials)).build();
      } else {
        s3 = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials))
            .withRegion(awsRegion).build();
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new SeisException("Cannot load AWS credentials from the credential profiles file. "
          + "Please make sure that your credentials file is at the correct "
          + "location (~/.aws/credentials), and is in valid format.", e);
    }
  }
  
  public JsAwsS3() {
    try {
      s3 = AmazonS3ClientBuilder.standard().build();
    } catch (Exception e) {
      e.printStackTrace();
      throw new IllegalStateException("Failure: Could not obtain S3 client",e);
    }
    awsProfile = "default";
    awsRegion = "default";
  }

  public void openRemote(String bucket, String prefix) throws SeisException {
    String key = prefix + "/" + FILE_PROPERTIES_JSC;
    if (s3.doesObjectExist(bucket, key) == false)
      throw new SeisException("Could not find JS AWS-S3 FileProperties: s3://" + awsBucket + "/" + key);    
    jscFileProperties = (JscFileProperties) getJsonObject(s3,bucket,key, JscFileProperties.class);
    this.awsBucket = bucket;
    this.awsPrefix = prefix;
    loadProperties();
  }

  public void loadProperties() throws SeisException {
    // Get data format and allocate arrays and buffers for traces and headers
    GridDefinition grid = jscFileProperties.gridDefinition;
    nsamp = (int) grid.getNumSamplesPerTrace();
    recordLength = TraceCompressor.getRecordLength(jscFileProperties.traceFormat, nsamp);
    maxTraces = (int) grid.getNumTracesPerFrame();
    // We use a byte array and buffer to hold traces and headers
    trcBytes = new byte[recordLength * maxTraces];
    trcBuffer = ByteBuffer.wrap(trcBytes);
    traceCompressor = new TraceCompressor(jscFileProperties.traceFormat, nsamp, trcBuffer);
    if (jscFileProperties.usesTraceProperties) {
      hdrLength = jscFileProperties.traceProperties.getHeaderLength();
      // Round header length to an even word boundary
      int rem = hdrLength % 4;
      if (rem != 0)
        hdrLength += rem;
      hdrWords = hdrLength / 4;
      hdrBytes = new byte[hdrLength * maxTraces];
      hdrBuffer = ByteBuffer.wrap(hdrBytes);
      jscFileProperties.traceProperties.setBuffer(hdrBuffer);
      hdrBuffer.order(jscFileProperties.byteOrder);
      intBuffer = hdrBuffer.asIntBuffer();
    }
    isOpen = true;
    volRange = new int[3];
    volRange[0] = (int) grid.getAxisLogicalOrigin(3);
    volRange[2] = (int) grid.getAxisLogicalDelta(3);
    volRange[1] = volRange[0] + (int) (grid.getAxisLength(3) - 1) * volRange[2];
    frmRange = new int[3];
    frmRange[0] = (int) grid.getAxisLogicalOrigin(2);
    frmRange[2] = (int) grid.getAxisLogicalDelta(2);
    frmRange[1] = frmRange[0] + (int) (grid.getAxisLength(2) - 1) * frmRange[2];
    frameCount = (int) (grid.getAxisLength(2) * grid.getAxisLength(3));
  }

  public float[][] allocateTraceArray() {
    return new float[maxTraces][nsamp];
  }

  public int[][] allocateHeaderArray() {
    return new int[maxTraces][hdrWords];
  }

  public boolean frameExists(int[] pos) {
    String key = awsPrefix + "/Traces" + "/V" + pos[3] + "/F" + pos[2];
    return s3.doesObjectExist(awsBucket, key);
  }

  /**
   * Write traces and headers to an open JavaSeis AWS-S3 dataset
   * 
   * @param ntrc - number of traces to write
   * @param trcs - 2D float array containing traces
   * @param hdrs - 2D int array containing headers
   * @param pos  - file position where data will be written
   * @throws SeisException - on AWS or IO errors
   */
  public void putFrame(int ntrc, float[][] trcs, int[][] hdrs, int[] pos) throws SeisException {
    putFrameHeaders(ntrc, hdrs, pos[2], pos[3]);
    putFrameTraces(ntrc, trcs, pos[2], pos[3]);
  }

  /**
   * Write traces and headers to an open JavaSeis AWS-S3 dataset
   * 
   * @param ntrc - number of traces to write
   * @param trcs - 2D float array containing traces
   * @param hdrs - 2D int array containing headers
   * @param pos  - file position where data will be written
   * @throws SeisException - on AWS or IO errors
   */
  public int getFrame(float[][] trcs, int[][] hdrs, int[] pos) throws SeisException {
    if (!frameExists(pos))
      return 0;
    getFrameHeaders(hdrs, pos[2], pos[3]);
    int ntrc = getFrameTraces(trcs, pos[2], pos[3]);
    return ntrc;
  }

  /**
   * Store traces in an AWS-S3 dataset
   * 
   * @param ntrc  - number of traces to write
   * @param frame - 2D float array containing traces
   * @param pos   - file position where data will be written
   * @throws SeisException - on AWS or IO errors
   */
  public void putFrameTraces(int ntrc, float[][] frame, int frameIndex, int volumeIndex) throws SeisException {
    String key = awsPrefix + "/Traces" + "/V" + volumeIndex + "/F" + frameIndex;
    try {
      traceCompressor.packFrame(ntrc, frame);
      InputStream is = new ByteArrayInputStream(trcBytes);
      ObjectMetadata om = new ObjectMetadata();
      om.setContentLength(ntrc * recordLength);
      om.addUserMetadata("traceCount", Integer.toString(ntrc));
      s3.putObject(awsBucket, key, is, om);
    } catch (Exception e) {
      e.printStackTrace();
      throw new SeisException("JsAwsS3 putFrameTraces failed: ", e.getCause());
    }
  }

  /**
   * Retrieve traces from an AWS-S3 dataset
   * 
   * @param frame - 2D float array containing output traces
   * @param pos   - file position for the read
   * @return - number of traces retrieved
   * @throws SeisException - on AWS or IO errors
   */
  public int getFrameTraces(float[][] frame, int frameIndex, int volumeIndex) throws SeisException {
    String key = awsPrefix + "/Traces" + "/V" + volumeIndex + "/F" + frameIndex;
    int traceCount = 0;
    if (s3.doesObjectExist(awsBucket, key) == false)
      return 0;
    try {
      GetObjectRequest gor = new GetObjectRequest(awsBucket, key);
      S3Object s3o = s3.getObject(gor);
      traceCount = Integer.parseInt(s3o.getObjectMetadata().getUserMetaDataOf("traceCount"));
      InputStream is = s3o.getObjectContent();
      int count = 0;
      int len = 0;
      while ((len = is.read(trcBytes, count, 16384)) > 0)
        count += len;
      s3o.close();
      traceCompressor.unpackFrame(traceCount, frame);
    } catch (Exception e) {
      e.printStackTrace();
      throw new SeisException("JsAwsS3 getFrameTraces failed: ", e.getCause());
    }
    return traceCount;
  }

  /**
   * Store headers in an AWS-S3 dataset
   * 
   * @param ntrc - number of traces to write
   * @param hdrs - 2D int array containing headers
   * @param pos  - file position where data will be written
   * @throws SeisException - on AWS or IO errors
   */
  public void putFrameHeaders(int trcCount, int[][] hdrs, int frameIndex, int volumeIndex) throws SeisException {
    String key = awsPrefix + "/Headers" + "/V" + volumeIndex + "/F" + frameIndex;
    try {
      intBuffer.clear();
      for (int j = 0; j < trcCount; j++) {
        intBuffer.put(hdrs[j], 0, hdrWords);
      }
      InputStream is = new ByteArrayInputStream(hdrBytes);
      ObjectMetadata om = new ObjectMetadata();
      om.setContentLength(trcCount * hdrLength);
      om.addUserMetadata("traceCount", Integer.toString(trcCount));
      s3.putObject(awsBucket, key, is, om);
    } catch (Exception e) {
      e.printStackTrace();
      throw new SeisException("JsAwsS3 putFrameHeaders failed: ", e.getCause());
    }
  }

  public void putFrameProperties(int trcCount, TracePropertiesImpl tp, int frameIndex, int volumeIndex)
      throws SeisException {
    String key = awsPrefix + "/Headers" + "/V" + volumeIndex + "/F" + frameIndex;
    try {
      ByteBuffer inBuffer = tp.getBuffer();
      inBuffer.rewind();
      if (tp.getHeaderLength() != tp.getRecordLength()) {
        int inLength = tp.getRecordLength();
        int outLength = tp.getHeaderLength();
        hdrBuffer.clear();
        for (int j = 0; j < trcCount; j++) {
          copyBufferToBuffer(inBuffer, inLength * j, hdrBuffer, outLength * j, outLength);
        }
        hdrBuffer.rewind();
        inBuffer = hdrBuffer;
      }
      InputStream is = new ByteBufferBackedInputStream(inBuffer);
      ObjectMetadata om = new ObjectMetadata();
      om.setContentLength(trcCount * hdrLength);
      om.addUserMetadata("traceCount", Integer.toString(trcCount));
      s3.putObject(awsBucket, key, is, om);
    } catch (Exception e) {
      e.printStackTrace();
      throw new SeisException("JsAwsS3 putFrameHeaders failed: ", e.getCause());
    }
  }

  void copyBufferToBuffer(ByteBuffer inBuffer, int inOffset, ByteBuffer outBuffer, int outOffset, int length) {
    inBuffer.position(inOffset);
    inBuffer.limit(inOffset + length);
    outBuffer.position(outOffset);
    outBuffer.put(inBuffer);
  }

  /**
   * Retrieve headers from an AWS-S3 dataset
   * 
   * @param hdrs - 2D int array containing output headers
   * @param pos  - file position for the read
   * @throws SeisException - on AWS or IO errors
   */
  public int getFrameHeaders(int[][] hdrs, int frameIndex, int volumeIndex) throws SeisException {
    String key = awsPrefix + "/Headers" + "/V" + volumeIndex + "/F" + frameIndex;
    int traceCount = 0;
    intBuffer.clear();
    try {
      GetObjectRequest gor = new GetObjectRequest(awsBucket, key);
      S3Object s3o = s3.getObject(gor);
      traceCount = Integer.parseInt(s3o.getObjectMetadata().getUserMetaDataOf("traceCount"));
      InputStream is = s3o.getObjectContent();
      int count = 0;
      int len = 0;
      while ((len = is.read(hdrBytes, count, 16384)) > 0)
        count += len;
      s3o.close();
      for (int j = 0; j < traceCount; j++) {
        intBuffer.get(hdrs[j], 0, hdrWords);
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new SeisException("JsAwsS3 getFrame failed: ", e.getCause());
    }
    return traceCount;
  }

  /**
   * Retrieve headers from an AWS-S3 dataset
   * 
   * @param hdrs - 2D int array containing output headers
   * @param pos  - file position for the read
   * @throws SeisException - on AWS or IO errors
   */
  public TraceProperties getFrameProperties( int frameIndex, int volumeIndex) throws SeisException {
    getFrameProperties(jscFileProperties.traceProperties, frameIndex, volumeIndex);
    return jscFileProperties.traceProperties;
  }
  
  public int getFrameProperties(TracePropertiesImpl tp, int frameIndex, int volumeIndex) throws SeisException {
    String key = awsPrefix + "/Headers" + "/V" + volumeIndex + "/F" + frameIndex;
    try {
      GetObjectRequest gor = new GetObjectRequest(awsBucket, key);
      S3Object s3o = s3.getObject(gor);
      int traceCount = Integer.parseInt(s3o.getObjectMetadata().getUserMetaDataOf("traceCount"));
      int maxbytes = traceCount * tp.getRecordLength();
      InputStream is = s3o.getObjectContent();
      ByteBuffer outBuffer = tp.getBuffer();
      outBuffer.rewind();
      int len = 0;
      int count = 0;
      while ((len = is.read(xfrBytes, 0, Math.min(16384, maxbytes - count))) > 0) {
        outBuffer.put(xfrBytes, 0, len);
        count += len;
      }
      com.amazonaws.util.IOUtils.drainInputStream(is);
      s3o.close();
      return traceCount;
    } catch (Exception e) {
      e.printStackTrace();
      throw new SeisException("JsAwsS3 getFrame failed: ", e.getCause());
    }
  }

  public void setPosition(int[] position) {
    System.arraycopy(position, 0, pos, 0, 4);
  }

  /**
   * Store an object as a json string in the current JavaSeis AWS-S3 Bucket/Prefix
   * location
   * 
   * @param objectName - name to append to Bucket/Prefix
   * @param filePath   - Path to file to be stored
   * @throws SeisException - on I/O or AWS errors
   */
  public static void putJsonObject(AmazonS3 awsS3, String bucket, String key, Object obj, boolean overwrite) throws SeisException {
    if (awsS3.doesObjectExist(bucket,key) == true && overwrite == false)
      throw new SeisException("JsAwsS3 putObject failed, object already exists: s3://" + bucket + "/" + key);
    try {
      byte[] bytes = JsonUtil.toJsonString(obj).getBytes();
      ObjectMetadata om = new ObjectMetadata();
      om.setContentLength(bytes.length);
      InputStream is = new ByteArrayInputStream(bytes);
      awsS3.putObject(bucket, key, is, om);
    } catch (Exception e) {
      throw new SeisException("JsAwsS3 putObject failed for:  s3://" + bucket + "/" + key, e.getCause());
    }
  }

  /**
   * Store an object as a json string in the current JavaSeis AWS-S3 Bucket/Prefix
   * location
   * 
   * @param objectName - name to append to Bucket/Prefix
   * @param filePath   - Path to file to be stored
   * @throws SeisException - on I/O or AWS errors
   */
  public static Object getJsonObject(AmazonS3 awsS3, String bucket, String key, Class<?> objClass) throws SeisException {
    if (awsS3.doesObjectExist(bucket, key) == false)
      throw new SeisException("JsAwsS3 getJsonObject failed, object does not exist: s3://" + bucket + "/" + key);
    try {
      GetObjectRequest gor = new GetObjectRequest(bucket, key);
      S3Object s3o = awsS3.getObject(gor);
      int maxbytes = (int) s3o.getObjectMetadata().getContentLength();
      byte[] bytes = new byte[maxbytes];
      InputStream is = s3o.getObjectContent();
      int len = 0;
      int count = 0;
      while ((len = is.read(bytes, count, Math.min(16384, maxbytes - count))) > 0) {
        count += len;
      }
      com.amazonaws.util.IOUtils.drainInputStream(is);
      s3o.close();
      return JsonUtil.fromJsonString(objClass, new String(bytes));
    } catch (Exception e) {
      throw new SeisException("JsAwsS3 getObject failed for: s3://" + bucket + "/" + key, e);
    }
  }

  /**
   * Store a file in the current JavaSeis AWS-S3 Bucket/Prefix location
   * 
   * @param objectName - name to append to Bucket/Prefix
   * @param filePath   - Path to file to be stored
   * @throws SeisException - on I/O or AWS errors
   */
  public static void putFile(AmazonS3 s3handle, String bucket, String key, String filePath, boolean overwrite) throws SeisException {
    if (s3handle.doesObjectExist(bucket, key) == true && overwrite == false)
      throw new SeisException("JsAwsS3 putFile failed, object already exists s3://" + bucket + "/" + key);
    try {
      File f = new File(filePath);
      InputStream is = new FileInputStream(f);
      ObjectMetadata om = new ObjectMetadata();
      om.setContentLength(f.length());
      s3handle.putObject(bucket, key, is, om);
    } catch (Exception e) {
      e.printStackTrace();
      throw new SeisException("JsAwsS3 putFile failed for file: " + filePath, e.getCause());
    }
  }

  /**
   * Retrieve a file from the current JavaSeis AWS-S3 Bucket/Prefix location
   * 
   * @param objectName - name of the object to be retrieved
   * @param filePath   - Path to file to be stored
   * @throws SeisException - on I/O or AWS errors
   */
  public void getTextFile(String objectName, String filePath) throws SeisException {
    String key = awsPrefix + "/" + objectName;
    if (s3.doesObjectExist(awsBucket, key) == false)
      throw new SeisException("Could not find object " + objectName + " in path: s3://" + awsBucket + "/" + awsPrefix);
    S3Object fileObj = s3.getObject(new GetObjectRequest(awsBucket, key));
    try {
      BufferedReader reader = new BufferedReader(new InputStreamReader(fileObj.getObjectContent()));
      BufferedWriter writer = new BufferedWriter(new FileWriter(filePath));
      String line = null;
      while ((line = reader.readLine()) != null) {
        writer.append(line + "\n");
      }
      writer.close();
      reader.close();
    } catch (Exception e) {
      throw new SeisException("Error while writing to: " + filePath);
    }
  }

  public String getAwsBucket() {
    return awsBucket;
  }

  public String getRegion() {
    return awsRegion;
  }

  public String getAwsPrefix() {
    return awsPrefix;
  }

  public int getFrameCount() {
    return frameCount;
  }

  public int[] getVolumeRange() {
    return volRange;
  }

  public int[] getFrameRange() {
    return frmRange;
  }
  
  public int[] getShape() {
    int ndim = jscFileProperties.gridDefinition.getNumDimensions();
    int[] shape = new int[ndim];
    for (int i=0; i<ndim; i++) {
      shape[i] = (int) jscFileProperties.gridDefinition.getAxisLength(i);
    }
    return shape;
  }

  public GridDefinition getGridDefinition() {
    return jscFileProperties.gridDefinition;
  }

  public TraceProperties traceProperties() {
    return jscFileProperties.traceProperties;
  }

  public BinGrid getBinGrid() {
    return jscFileProperties.binGrid;
  }

  public Instant getTimeZero() {
    return jscFileProperties.timeZero;
  }
  
  public void setTimeZero( Instant t0 ) {
    jscFileProperties.timeZero = t0;
  }
  
  public static int bufLen = 16384;
  public static byte[] readBuf = new byte[bufLen];

  /**
   * Convenience method to read an input stream to a ByteBuffer
   * 
   * @param inputStream - source
   * @param buf         - destination
   * @return - number of bytes transferred
   * @throws IOException
   */
  public static int readToByteBuffer(InputStream inputStream, ByteBuffer buf) throws IOException {
    int readLen = 0;
    int count = 0;
    while ((readLen = inputStream.read(readBuf, 0, bufLen)) != -1) {
      buf.put(readBuf, 0, readLen);
      count += readLen;
    }
    return count;
  }

  public static void main(String[] args) {
    
    System.out.println("Success");
  }

}
