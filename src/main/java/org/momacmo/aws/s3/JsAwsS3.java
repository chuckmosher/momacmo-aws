package org.momacmo.aws.s3;

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
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.javaseis.compress.TraceCompressor;
import org.javaseis.grid.GridDefinition;
import org.javaseis.parset.ParameterSetIO;
import org.javaseis.properties.DataFormat;
import org.javaseis.properties.TraceProperties;
import org.javaseis.util.SeisException;

import org.momacmo.javaseis.properties.PropertiesTree;
import org.momacmo.javaseis.util.GridUtil;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;

import edu.mines.jtk.util.ArrayMath;
import edu.mines.jtk.util.ParameterSet;

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
  // SeisSpace properties
  PropertiesTree propertiesTree = null;
  ParameterSet filePropertiesParset = null;
  ParameterSet tracePropertiesParset = null;
  TraceProperties traceProperties = null;
  ParameterSet binProperties = null;
  static String SS_AWS_S3_PROPERTIES = "/SeisSpaceAwsS3.xml";
  static String FILE_PROPERTIES = "FileProperties.xml";
  // AWS bucket, prefix, and client
  String awsProfile, awsRegion;
  String awsBucket;
  String awsPrefix;
  AmazonS3 s3;
  // Runtime objects used internally
  GridDefinition gridDefinition;
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
    File fj = new File(jsPath + SS_AWS_S3_PROPERTIES);
    if (!fj.exists())
      return false;
    return true;
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
    if (s3Tmp.doesObjectExist(awsBucketName, awsPrefixName + "/" + FILE_PROPERTIES) == false) {
      s3Tmp.shutdown();
      return false;
    }
    s3Tmp.shutdown();
    return true;
  }

  /**
   * Create a JavaSeis AWS-S3 dataset using an existing 'empty' SeisSpace dataset.
   * The dataset should have been created in SeisSpace, but any data written in
   * the dataset other than metadata will be ignored.
   * 
   * @param awsBucketName   - AWS Bucket where the trace and header data will be
   *                        written
   * @param awsPrefixString - AWS Prefix within the bucket
   * @param jsPath          - full path to the SeisSpace dataset on local disk
   * @param overWrite       - set to true to over-write existing JavaSeis AWS-S3
   *                        metadata
   * @throws SeisException - on AWS access and I/O errors
   */
  public static void createFromExisting(String awsProfileName, String awsRegionName, String awsBucketName, String jsPath, boolean overwrite )
      throws SeisException {
    AWSCredentials credentials = null;
    AmazonS3 s3Tmp = null;
    try {
      credentials = new ProfileCredentialsProvider(awsProfileName).getCredentials();
      if (awsRegionName == null || awsRegionName.equals("default")) {
        s3Tmp = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials)).build();
      } else {
        s3Tmp = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials))
            .withRegion(awsRegionName).build();
      }
    } catch (Exception e) {
      e.printStackTrace();
     throw new SeisException("Cannot load AWS credentials from the credential profiles file. "
          + "Please make sure that your credentials file is at the correct "
          + "location (~/.aws/credentials), and is in valid format.",e.getCause());
    }
    if (s3Tmp.doesBucketExistV2(awsBucketName) == false) {
      s3Tmp.shutdown();
      throw new SeisException("Bucket does not exist: " + awsBucketName);
    }
    String awsPrefixName = getAwsPrefixFromPath(jsPath);
    if (awsPrefixName == null) {
      s3Tmp.shutdown();
      throw new SeisException("Could not construct AWS Prefix from path name: " + jsPath);
    }
    if (s3Tmp.listObjectsV2(awsBucketName, awsPrefixName).getKeyCount() > 0 && overwrite == false)
      throw new SeisException("Bucket/Prefix exists and overWrite is false: " + awsBucketName + "/" + awsPrefixName);
    // Create a parameter set and write to local disk
    ParameterSet ssAwsS3Parms = new ParameterSet("SeisSpaceAwsS3");
    ssAwsS3Parms.setString("awsRegion", awsRegionName);
    ssAwsS3Parms.setString("awsBucket", awsBucketName);
    ssAwsS3Parms.setString("awsPrefix", awsPrefixName);
    try {
      ParameterSetIO.writeFile(ssAwsS3Parms, jsPath + SS_AWS_S3_PROPERTIES);
      // Copy FileProperties.xml to the AWS S3 Bucket / Prefix
      putFile(s3Tmp, awsBucketName, awsPrefixName + "/" + FILE_PROPERTIES, jsPath + "/" + FILE_PROPERTIES);
    } catch (Exception e) {
      e.printStackTrace();
      throw new SeisException("Faild to create JavaSeis AWS-S3 Metadata");
    }
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
   * @param args - String array containing:
   * [0] - AWS Profile
   * [1] = AWS Region
   * [2] - AWS Bucket
   * [3] - AWS Prefix
   * @throws SeisException - on access errors
   */
  public JsAwsS3(String[] args) throws SeisException {
    this(args[0],args[1]);
    openRemote(args[2],args[3]);
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
    if (awsRegionName == null) awsRegionName = "default";
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

  /**
   * Open an existing JavaSeis AWS-S3 dataset
   * 
   * @param jsFilePath - path to the JavaSeis 'stub' file on local disk
   * @throws SeisException - on IO or AWS access errors
   */
  public void open(String jsFilePath) throws SeisException {
    // Load file properties and retrieve metadata
    propertiesTree = new PropertiesTree(jsFilePath, true);
    ParameterSet ps;
    try {
      ps = ParameterSetIO.readFile(jsFilePath + SS_AWS_S3_PROPERTIES);
    } catch (Exception e) {
      e.printStackTrace();
      throw new SeisException("Could not read " + SS_AWS_S3_PROPERTIES, e.getCause());
    }
    String buf = ps.getString("awsRegion", "default");
    /*
     * if (!buf.equals(awsRegion)) { throw new SeisException("Current region " +
     * awsRegion + " does not match file region " + buf); }
     */    awsBucket = ps.getString("awsBucket", "null");
    if (awsBucket.equals("null"))
      throw new SeisException("Could not find AWS Bucket Name");
    awsPrefix = ps.getString("awsPrefix", "null");
    if (awsPrefix.equals("null"))
      throw new SeisException("Could not find AWS Prefix Name");
    loadProperties();
  }

  public void loadProperties() throws SeisException {
    // Load file properties and trace properties
    filePropertiesParset = propertiesTree.getFileProperties();
    tracePropertiesParset = propertiesTree.getTraceProperties();
    traceProperties = new TraceProperties(tracePropertiesParset);
    // Construct grid definition from FileProperties
    gridDefinition = GridUtil.fromParameterSet(filePropertiesParset);
    // Get data format and allocate arrays and buffers for traces and headers
    DataFormat traceFormat = DataFormat.valueOf(filePropertiesParset.getString("TraceFormat", "FLOAT"));
    nsamp = (int) gridDefinition.getNumSamplesPerTrace();
    recordLength = TraceCompressor.getRecordLength(traceFormat, nsamp);
    maxTraces = (int) gridDefinition.getNumTracesPerFrame();
    // We use a byte array and buffer to hold traces and headers
    trcBytes = new byte[recordLength * maxTraces];
    trcBuffer = ByteBuffer.wrap(trcBytes);
    traceCompressor = new TraceCompressor(traceFormat, nsamp, trcBuffer);
    hdrLength = filePropertiesParset.getInt("HeaderLengthBytes", 0);
    // Round header length to an even word boundary
    int rem = hdrLength % 4;
    if (rem != 0)
      hdrLength += rem;
    hdrWords = hdrLength / 4;
    hdrBytes = new byte[hdrLength * maxTraces];
    hdrBuffer = ByteBuffer.wrap(hdrBytes);
    traceProperties.setBuffer(hdrBuffer);
    // SeisSpace uses integer arrays to hold headers so we create an 'Int' view
    hdrBuffer.order(ByteOrder.LITTLE_ENDIAN);
    intBuffer = hdrBuffer.asIntBuffer();
    isOpen = true;
    volRange = new int[3];
    volRange[0] = (int) gridDefinition.getAxisLogicalOrigin(3);
    volRange[2] = (int) gridDefinition.getAxisLogicalDelta(3);
    volRange[1] = volRange[0] + (int) (gridDefinition.getAxisLength(3) - 1) * volRange[2];
    frmRange = new int[3];
    frmRange[0] = (int) gridDefinition.getAxisLogicalOrigin(2);
    frmRange[2] = (int) gridDefinition.getAxisLogicalDelta(2);
    frmRange[1] = frmRange[0] + (int) (gridDefinition.getAxisLength(2) - 1) * frmRange[2];
    frameCount = (int) (gridDefinition.getAxisLength(2) * gridDefinition.getAxisLength(3));
  }  
  
  public void openRemote(String awsBucketName, String awsPrefixString) throws SeisException {
    awsBucket = awsBucketName;
    awsPrefix = awsPrefixString;
    String objectName = awsPrefix + "/" + FILE_PROPERTIES;
    if (s3.doesObjectExist(awsBucket, objectName) == false)
      throw new SeisException("Could not find AWS object: " + objectName);
    GetObjectRequest gor = new GetObjectRequest(awsBucket, objectName);
    S3Object s3o = s3.getObject(gor);
    InputStream is = s3o.getObjectContent();
    propertiesTree = new PropertiesTree(is);
    loadProperties();
  }


  public void openRemote(String awsBucketName, String awsPrefixString, String dataDir) throws SeisException {
    File fdata = new File(dataDir);
    if (!(fdata.isDirectory() && fdata.canWrite()))
      throw new SeisException("Cannot write to dataDir " + dataDir);
    awsBucket = awsBucketName;
    awsPrefix = awsPrefixString;
    String objectName = awsPrefix + "/" + FILE_PROPERTIES;
    if (s3.doesObjectExist(awsBucket, objectName) == false)
      throw new SeisException("Could not find JS AWS-S3 FileProperties: s3://" + awsBucket + "/" + objectName);
    String fileName = dataDir + "/" + FILE_PROPERTIES;
    getTextFile(FILE_PROPERTIES, fileName);
    propertiesTree = new PropertiesTree(dataDir);
    loadProperties();
  }

  public void openLocal(String awsBucketName, String awsPrefixString, String dataDir) throws SeisException {
    File fdata = new File(dataDir);
    if (!(fdata.isDirectory() && fdata.canRead()))
      throw new SeisException("Cannot read from dataDir " + dataDir);
    awsBucket = awsBucketName;
    awsPrefix = awsPrefixString;
    String objectName = awsPrefix + "/" + FILE_PROPERTIES;
    if (s3.doesObjectExist(awsBucket, objectName) == false)
      throw new SeisException("Could not find AWS object: " + objectName);
    propertiesTree = new PropertiesTree(dataDir);
    loadProperties();
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
    putFrameHeaders(ntrc, hdrs, pos);
    putFrameTraces(ntrc, trcs, pos);
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
    getFrameHeaders(hdrs, pos);
    int ntrc = getFrameTraces(trcs, pos);
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
  public void putFrameTraces(int ntrc, float[][] frame, int[] pos) throws SeisException {
    String key = awsPrefix + "/Traces" + "/V" + pos[3] + "/F" + pos[2];
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
  public int getFrameTraces(float[][] frame, int[] pos) throws SeisException {
    String key = awsPrefix + "/Traces" + "/V" + pos[3] + "/F" + pos[2];
    int traceCount = 0;
    if (s3.doesObjectExist(awsBucket, key) == false) return 0;
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
  public void putFrameHeaders(int trcCount, int[][] hdrs, int[] pos) throws SeisException {
    String key = awsPrefix + "/Headers" + "/V" + pos[3] + "/F" + pos[2];
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

  public void putFrameHeaders(int trcCount, TraceProperties tp, int[] pos) throws SeisException {
    String key = awsPrefix + "/Headers" + "/V" + pos[3] + "/F" + pos[2];
    try {
      InputStream is = new ByteBufferBackedInputStream(tp.getBuffer());
      ObjectMetadata om = new ObjectMetadata();
      om.setContentLength(trcCount * hdrLength);
      om.addUserMetadata("traceCount", Integer.toString(trcCount));
      s3.putObject(awsBucket, key, is, om);
    } catch (Exception e) {
      e.printStackTrace();
      throw new SeisException("JsAwsS3 putFrameHeaders failed: ", e.getCause());
    }
  }

  /**
   * Retrieve headers from an AWS-S3 dataset
   * 
   * @param hdrs - 2D int array containing output headers
   * @param pos  - file position for the read
   * @throws SeisException - on AWS or IO errors
   */
  public int getFrameHeaders(int[][] hdrs, int[] pos) throws SeisException {
    String key = awsPrefix + "/Headers" + "/V" + pos[3] + "/F" + pos[2];
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
  public TraceProperties getFrameProperties(int[] pos) throws SeisException {
    String key = awsPrefix + "/Headers" + "/V" + pos[3] + "/F" + pos[2];
    hdrBuffer.clear();
    try {
      GetObjectRequest gor = new GetObjectRequest(awsBucket, key);
      S3Object s3o = s3.getObject(gor);
      InputStream is = s3o.getObjectContent();
      int count = 0;
      int len = 0;
      while ((len = is.read(hdrBytes, count, 16384)) > 0)
        count += len;
      s3o.close();
    } catch (Exception e) {
      e.printStackTrace();
      throw new SeisException("JsAwsS3 getFrame failed: ", e.getCause());
    }
    return traceProperties;
  }
  
  public void setPosition(int[] position) {
    System.arraycopy(position, 0, pos, 0, 4 );
  }

  /**
   * Store a file in the current JavaSeis AWS-S3 Bucket/Prefix location
   * 
   * @param objectName - name to append to Bucket/Prefix
   * @param filePath   - Path to file to be stored
   * @throws SeisException - on I/O or AWS errors
   */
  public void putFile(String objectName, String filePath) throws SeisException {
    String key = awsPrefix + "/" + objectName;
    try {
      File f = new File(filePath);
      InputStream is = new FileInputStream(f);
      ObjectMetadata om = new ObjectMetadata();
      om.setContentLength(f.length());
      s3.putObject(awsBucket, key, is, om);
    } catch (Exception e) {
      e.printStackTrace();
      throw new SeisException("JsAwsS3 putFile failed for file: " + filePath, e.getCause());
    }
  }
  

  /**
   * Store a file in the current JavaSeis AWS-S3 Bucket/Prefix location
   * 
   * @param objectName - name to append to Bucket/Prefix
   * @param filePath   - Path to file to be stored
   * @throws SeisException - on I/O or AWS errors
   */
  public static void putFile(AmazonS3 s3handle, String bucket, String key, String filePath) throws SeisException {
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
        writer.append(line+"\n");
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

  public PropertiesTree getPropertiesTree() {
    return propertiesTree;
  }

  public GridDefinition getGridDefinition() {
    return gridDefinition;
  }

  public TraceProperties traceProperties() {
    return traceProperties;
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

  public static String[] getLogicalRangeStrings(String path) {
    String[] nullRanges = new String[] { "null", "null", "null", "null" };
    if (isJsAwsS3(path) == false)
      return nullRanges;
    PropertiesTree props = null;
    try {
      props = new PropertiesTree(path);
    } catch (SeisException e) {
      return nullRanges;
    }
    ParameterSet fileProperties = props.getFileProperties();
    GridDefinition grid = GridUtil.fromParameterSet(fileProperties);
    int n = grid.getNumDimensions();
    long[] lo = grid.getAxisLogicalOrigins();
    long[] ld = grid.getAxisLogicalDeltas();
    long[] l = grid.getAxisLengths();
    String[] ranges = new String[n];
    for (int i = 0; i < n; i++) {
      ranges[i] = String.format("%d,%d,%d", lo[i], lo[i] + ld[i] * (l[i] - 1), ld[i]);
    }
    for (int i = n; i < 4; i++) {
      ranges[i] = "null";
    }
    return ranges;
  }

  public static void main(String[] args) {
    float EPS = 1f / 16384f;
    try {
      //String path = "/data_home/meagerdas/prod/example.js";
      String path = "/transferspace/momacmo/meagerdas/test/110aws-s3test.js";
      JsAwsS3.createFromExisting("default","default","momacmos3", path, true);
      JsAwsS3 sss3 = new JsAwsS3("default");
      sss3.open(path);
      int nframe = (int) sss3.gridDefinition.getAxisLength(2);
      int ntrc = (int) sss3.gridDefinition.getAxisLength(1);
      int ns = (int) sss3.gridDefinition.getAxisLength(0);
      int hdrWords = sss3.hdrWords;
      float[][] frame = new float[ntrc][ns];
      int[][] hdrs = new int[ntrc][hdrWords];
      int[] pos = new int[4];
      int[] frange = sss3.getFrameRange();
      int[] vrange = sss3.getVolumeRange();
      pos[3] = vrange[0];
      nframe = 10;
      for (int k = 0; k < nframe; k++) {
        int val = frange[0] + frange[2] * k;
        ArrayMath.fill(val, frame);
        ArrayMath.fill(val, hdrs);
        pos[2] = val;
        System.out.println("Write headers for frame " + val);
        sss3.putFrameHeaders(ntrc, hdrs, pos);
        System.out.println("Write traces for frame " + val);
        sss3.putFrameTraces(ntrc, frame, pos);
      }
      System.out.println("Output complete");
      sss3.shutdown();
      System.out.println(Arrays.toString(JsAwsS3.getLogicalRangeStrings(path)));
      System.out.println("Open and read");
      sss3 = new JsAwsS3("default");
      sss3.open(path);
      for (int k = 0; k < nframe; k++) {
        int val = frange[0] + frange[2] * k;
        pos[2] = val;
        ArrayMath.fill(0, frame);
        ArrayMath.fill(0, hdrs);
        System.out.println("Read headers for frame " + val);
        sss3.getFrameHeaders(hdrs, pos);
        System.out.println("Read traces for frame " + val);
        sss3.getFrameTraces(frame, pos);
        for (int j = 0; j < ntrc; j++) {
          for (int i = 0; i < ns; i++) {
            if (Math.abs(frame[j][i] - val) > EPS) {
              System.out.println("Trace Out of range at: " + i + ", " + j + " value " + frame[j][i]);
              System.exit(-1);
            }
          }
          for (int i = 0; i < hdrWords; i++) {
            if (hdrs[j][i] != val) {
              System.out.println("Header Out of range at: " + i + ", " + j + " value " + hdrs[j][i]);
              System.exit(-1);
            }
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
    System.out.println("Success");
  }

}
