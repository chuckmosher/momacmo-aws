package org.momacmo.aws.lambda.tools;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.Future;

import org.javaseis.compress.TraceCompressor;
import org.javaseis.properties.DataFormat;
import org.javaseis.util.SeisException;
import org.momacmo.aws.s3.jscio.properties.JsonUtil;

import com.amazonaws.services.lambda.AWSLambdaAsync;
import com.amazonaws.services.lambda.AWSLambdaAsyncClientBuilder;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import com.amazonaws.services.lambda.model.ServiceException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

// Lambda handler for JavaSeisCloud Tools
public class JscLambdaTool implements RequestHandler<Map<String, String>, String> {
  @Override
  public String handleRequest(Map<String, String> event, Context context) {
    LambdaLogger logger = context.getLogger();
    JscLambdaInput input = new JscLambdaInput(event);
    logger.log("Lambda Function Inovked: " + this.getClass().getCanonicalName() + "::handleRequest");
    logger.log(JsonUtil.toJsonString(input));
    JscLambdaOutputList outputList = new JscLambdaOutputList(input);
    AmazonS3 s3 = null;
    try {
      s3 = AmazonS3ClientBuilder.standard().build();
    } catch (Exception e) {
      e.printStackTrace();
      String msg = "Failure:\n" + "Could not obtain S3 client\n" + e.getMessage() + "\n" + JsonUtil.toJsonString(input);
      logger.log(msg);
      throw new IllegalStateException(msg);
    }
    byte[] trcBytes = null;
    try {
      trcBytes = new byte[input.ntrace * TraceCompressor.getRecordLength(DataFormat.COMPRESSED_INT16, input.nsamp)];
    } catch (SeisException e) {
      e.printStackTrace();
      String msg = "Failure:\n" + "Could allocate trace buffers\n" + e.getMessage() + "\n" + JsonUtil.toJsonString(input);
      logger.log(msg);
      throw new IllegalStateException(msg);
    }
    getFrameRange( s3, input, outputList, trcBytes );
    logger.log("Lambda Function Completed: " + this.getClass().getCanonicalName() + "::handleRequest");
    String outputString = JsonUtil.toJsonString(outputList);
    logger.log("JscLambdaOutputList:\n" + outputString);
    return outputString;
  }

  public static JscLambdaOutput getFrameTraces(AmazonS3 s3, JscLambdaInput input, int frame, int volume,
      byte[] trcBytes) {
    JscLambdaOutput output = new JscLambdaOutput(frame, volume);
    String key = input.prefix + "/Traces" + "/V" + volume + "/F" + frame;
    if (s3.doesObjectExist(input.bucket, key) == false) {
      output.setStatus("Failure: Object not found: " + input.bucket + "/" + key);
      return output;
    }
    int traceCount = 0;
    int count = 0;
    int maxbytes = trcBytes.length;
    long tms = System.currentTimeMillis();
    try {
      GetObjectRequest gor = new GetObjectRequest(input.bucket, key);
      S3Object s3o = s3.getObject(gor);
      traceCount = Integer.parseInt(s3o.getObjectMetadata().getUserMetaDataOf("traceCount"));
      System.out.println("TraceCount " + traceCount);
      InputStream is = s3o.getObjectContent();
      int len = 0;
      while ((len = is.read(trcBytes, count, Math.min(16384, maxbytes - count))) > 0) {
        count += len;
      }
      com.amazonaws.util.IOUtils.drainInputStream(is);
      s3o.close();
      output.setStatus("Success");
      tms = System.currentTimeMillis() - tms;
      output.traceCount = traceCount;
      output.iobytes = count;
      output.iotime = 0.001f * tms;
    } catch (Exception e) {
      e.printStackTrace();
      output.setStatus("Failure: " + e.getMessage());
    }
    return output;
  }
  
  public static void getFrameRange(AmazonS3 s3, JscLambdaInput input, JscLambdaOutputList outputList,
      byte[] trcBytes) {
    for (int volume = input.vol0; volume <= input.voln; volume += input.voli) {
      for (int frame = input.frm0; frame <= input.frmn; frame += input.frmi) {
        JscLambdaOutput output = getFrameTraces(s3, input, frame, volume, trcBytes);
        outputList.add(output);
      }
    }
  }
  public static void main(String[] args) {
    JscLambdaInput input = new JscLambdaInput();
    input.setBucket("momacmos3");
    input.setPrefix("momacmo/meagerdas/1432_aws_output_filt_5_50_despike");
    input.setRange(401,405,1,11,11,1);
    input.setNsamp(1250);
    input.setNtrace(380);
    input.setBatchSize(10);

    InvokeRequest invokeRequest = new InvokeRequest().withFunctionName("JscProcessingWorker")
        .withPayload(JsonUtil.toJsonString(input));
    
    try {
      AWSLambdaAsync lambda = AWSLambdaAsyncClientBuilder.defaultClient();
      Future<InvokeResult> future = lambda.invokeAsync(invokeRequest);
      InvokeResult result = ToolInvoke.waitForResult( future );
      // write out the return value
      String rawOutput = Charset.defaultCharset().decode(result.getPayload()).toString();     
      System.out.println("Raw output:\n" + rawOutput);
      String outputString = rawOutput.replaceAll("\\\\n","").replaceAll("\\\\","");
      System.out.println("Processed output:\n" + outputString);
      JscLambdaOutputList output = (JscLambdaOutputList) JsonUtil.fromJsonString(JscLambdaOutputList.class, outputString.substring(1,outputString.length()-1));
      System.out.println("JscLambdaOutputList:\n" + JsonUtil.toJsonString(output));
      System.out.println("I/O Rate (KiB/s) " + 1e-3*output.getIoRate());
      System.exit(200);
    } catch (ServiceException e) {
      System.out.println(e);
      System.exit(500);
    }
  }
}