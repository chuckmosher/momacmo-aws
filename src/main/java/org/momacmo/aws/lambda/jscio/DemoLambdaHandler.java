package org.momacmo.aws.lambda.jscio;

import java.io.InputStream;
import java.util.Map;

import org.javaseis.compress.TraceCompressor;
import org.javaseis.properties.DataFormat;
import org.javaseis.util.SeisException;
import org.momacmo.javaseis.parameters.JsonUtil;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

// Handler value: example.Handler
public class DemoLambdaHandler implements RequestHandler<Map<String, String>, String> {
  Gson gson = new GsonBuilder().setPrettyPrinting().create();

  @Override
  public String handleRequest(Map<String, String> event, Context context) {
    LambdaLogger logger = context.getLogger();
    // log execution details
    logger.log("ENVIRONMENT VARIABLES: " + gson.toJson(System.getenv()));
    logger.log("CONTEXT: " + gson.toJson(context));
    // process event
    logger.log("EVENT: " + gson.toJson(event));
    logger.log("EVENT TYPE: " + event.getClass());
    JscLambdaInput input = new JscLambdaInput(event);
    JscLambdaOutput output = new JscLambdaOutput();
    output.setInput(input);
    // We use a byte array and buffer to hold traces and headers
    AmazonS3 s3 = null;
    try {
      logger.log("Get S3 client ... ");
      System.out.flush();
      s3 = AmazonS3ClientBuilder.standard().build();
      logger.log("... S3 client found");
    } catch (Exception e) {
      e.printStackTrace();
      output.setStatus("Could not obtain S3 client");
      return JsonUtil.toJsonString(output);
    }
    try {
      byte[] trcBytes = new byte[input.ntrace * TraceCompressor.getRecordLength(DataFormat.COMPRESSED_INT16, input.nsamp)];
      output = getFrameTraces(s3,input,trcBytes);
      output.setStatus("Success");
    } catch (Exception e) {
      e.printStackTrace();
      output.setStatus("Could not obtain S3 client");
      return JsonUtil.toJsonString(output);
    }
    return JsonUtil.toJsonString(output);
  }

  public static JscLambdaOutput getFrameTraces(AmazonS3 s3, JscLambdaInput input, byte[] trcBytes) throws SeisException {
    String key = input.prefix + "/Traces" + "/V" + input.volume + "/F" + input.frame;
    System.out.println("Look for key " + input.bucket + ":" + key);
    System.out.flush();
    if (s3.doesObjectExist(input.bucket, key) == false)
      return null;
    System.out.println("Found " + input.bucket + ":" + key);
    int traceCount = 0;
    int count = 0;
    long tms = System.currentTimeMillis();
    JscLambdaOutput output = new JscLambdaOutput();
    try {
      GetObjectRequest gor = new GetObjectRequest(input.bucket, key);
      S3Object s3o = s3.getObject(gor);
      traceCount = Integer.parseInt(s3o.getObjectMetadata().getUserMetaDataOf("traceCount"));
      System.out.println("TraceCount " + traceCount);
      InputStream is = s3o.getObjectContent();
      int len = 0;
      while ((len = is.read(trcBytes, count, 16384)) > 0) {
        count += len;
        System.out.println("Read " + len + " Total " + count);
      }
      s3o.close();
      output.setStatus("Success");
      tms = System.currentTimeMillis() - tms;
      output.input = input;
      output.traceCount = traceCount;
      output.iobytes = count;
      output.iotime = 0.001f * tms;
      System.out.println("Completed in " + tms + "msec");
      System.out.flush();
    } catch (Exception e) {
      e.printStackTrace();
      System.err.println(e.getMessage());
      output.setStatus("Error reading traces");
    }    
    return output;
  }
}