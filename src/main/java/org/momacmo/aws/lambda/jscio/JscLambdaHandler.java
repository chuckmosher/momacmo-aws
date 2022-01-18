package org.momacmo.aws.lambda.jscio;
import java.io.InputStream;

import org.javaseis.util.SeisException;
import org.momacmo.javaseis.parameters.JsonUtil;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.Region;
import com.amazonaws.services.s3.model.S3Object;

public class JscLambdaHandler implements RequestHandler<SQSEvent, JscLambdaOutput>{
  static byte[] trcBytes = new byte[16384];  
  @Override
    public JscLambdaOutput handleRequest(SQSEvent event, Context context)
    {
    	JscLambdaInput input = null;
        for(SQSMessage msg : event.getRecords()){
        	String body = msg.getBody();
        	input = (JscLambdaInput) JsonUtil.fromJsonString(JscLambdaInput.class, body);
            System.out.println(JsonUtil.toJsonString(input));
        }
        JscLambdaOutput output = new JscLambdaOutput();
        output.setInput(input);
        try {
          System.out.println("Get S3 client ... ");
          System.out.flush();
          AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
          System.out.println("... S3 client found");
          System.out.flush();
          output = getFrameTraces(s3, input);
          if ( output == null) {
            System.err.println("getFrameTraces failed");
          }
        } catch (Exception e) {
          e.printStackTrace();
          System.err.println("Could not obtain S3 client");
          return null;
        }
        return output;
    }
    
    public static JscLambdaOutput getFrameTraces(AmazonS3 s3, JscLambdaInput input ) throws SeisException {
      String key = input.prefix + "/Traces" + "/V" + input.volume + "/F" + input.frame;
      System.out.println("Look for key " + input.bucket + ":" + key);
      System.out.flush();
      if (s3.doesObjectExist(input.bucket, key) == false) return null;
      System.out.println("Found " + input.bucket + ":" + key);
      int traceCount = 0;
      int count = 0;
      long tms = System.currentTimeMillis();
      try {
        GetObjectRequest gor = new GetObjectRequest(input.bucket, key);
        S3Object s3o = s3.getObject(gor);
        traceCount = Integer.parseInt(s3o.getObjectMetadata().getUserMetaDataOf("traceCount"));
        System.out.println("TraceCount " + traceCount);
        InputStream is = s3o.getObjectContent();
        int len = 0;
        while ((len = is.read(trcBytes, 0, 16384)) > 0) {
          count += len;
          System.out.println("Read " + len + " Total " + count);
        }
        s3o.close();
      } catch (Exception e) {
        e.printStackTrace();
        System.err.println(e.getMessage());
        return null;
      }
      tms = System.currentTimeMillis() - tms;
      JscLambdaOutput output = new JscLambdaOutput();
      output.input = input;
      output.traceCount = traceCount;
      output.iobytes = count;
      output.iotime = 0.001f * tms;
      System.out.println("Completed in " + tms + "msec");
      System.out.flush();
      return output;
    }
    
    public static void main( String[] args ) {
      AWSCredentials credentials = null;
      AmazonS3 s3 = null;
      try {
        credentials = new ProfileCredentialsProvider("default").getCredentials();
       
        s3 = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials)).build();
        
      } catch (Exception e) {
        e.printStackTrace();
        System.out.println("Cannot load AWS credentials from the credential profiles file. "
            + "Please make sure that your credentials file is at the correct "
            + "location (~/.aws/credentials), and is in valid format.");
        return;
      }
    	JscLambdaInput input = new JscLambdaInput();
    	input.setBucket("momacmos3");
    	input.setPrefix("momacmo/meagerdas/1432_aws_output_filt_5_50_despike");
    	input.setVolume(11);
    	input.setFrame(403);
    	JscLambdaOutput output = null;
    	try {
        output = getFrameTraces(s3, input);
      } catch (SeisException e) {
        e.printStackTrace();
        System.err.println(e.getMessage());
        return;
      }
    	System.out.println(JsonUtil.toJsonString(output));
    }
}