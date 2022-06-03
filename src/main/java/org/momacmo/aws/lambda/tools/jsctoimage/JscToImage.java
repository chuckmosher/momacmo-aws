package org.momacmo.aws.lambda.tools.jsctoimage;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.Future;

import org.javaseis.util.SeisException;
import org.momacmo.aws.lambda.tools.ToolInvoke;
import org.momacmo.aws.s3.jscio.JsAwsS3;
import org.momacmo.aws.s3.jscio.properties.JsonUtil;

import com.amazonaws.services.lambda.AWSLambdaAsync;
import com.amazonaws.services.lambda.AWSLambdaAsyncClientBuilder;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import com.amazonaws.services.lambda.model.ServiceException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;

// Lambda handler for JavaSeisCloud Tools
public class JscToImage implements RequestHandler<Map<String, String>, String> {
  @Override
  public String handleRequest(Map<String, String> event, Context context) {
    long t0 = System.currentTimeMillis();
    LambdaLogger logger = context.getLogger();
    JscToImageInput input = new JscToImageInput(event);
    logger.log("Lambda Function Inovked: " + this.getClass().getCanonicalName() + "::handleRequest");
    logger.log(JsonUtil.toJsonString(input));
    String httpImage = null;
    try {
      JsAwsS3 sio = new JsAwsS3( );
      sio.openRemote( input.bucket, input.prefix );
      float[][] trcs = sio.allocateTraceArray();
      sio.getFrameTraces(trcs, input.frame, input.volume);
      FloatToImage ftoi = new FloatToImage(input.colorScale,trcs[0].length, trcs.length, input.scaleMin, input.scaleMax );
      ftoi.putFloats(trcs);
      httpImage = ftoi.getBase64Image();
    } catch (SeisException e) {
      e.printStackTrace();
      String msg = "Failure:\n" + e.getMessage() + "\n" + JsonUtil.toJsonString(input);
      logger.log(msg);
      throw new IllegalStateException(msg);
    }
    logger.log("Lambda Function Completed: " + this.getClass().getCanonicalName() + "::handleRequest");
    logger.log("Execution time: " + 0.001*(System.currentTimeMillis()-t0));
    return httpImage;
  }

  public static void main(String[] args) {
    JscToImageInput input = new JscToImageInput( "default", "default", "momacmos3", "momacmo/meagerdas/1432_aws_output_filt_5_50_despike", 
        205, 11, -50f, 50f, DisplayColorModel.ColorModel.BLACK_WHITE_RED);
    System.out.println(JsonUtil.toJsonString(input));
    
    InvokeRequest invokeRequest = new InvokeRequest().withFunctionName("JscToImage")
        .withPayload(JsonUtil.toJsonString(input));
    
    try {
      AWSLambdaAsync lambda = AWSLambdaAsyncClientBuilder.defaultClient();
      Future<InvokeResult> future = lambda.invokeAsync(invokeRequest);
      InvokeResult result = ToolInvoke.waitForResult( future );
      // write out the return value
      String rawOutput = Charset.defaultCharset().decode(result.getPayload()).toString();     
      System.out.println("Raw output:\n" + rawOutput);
      System.exit(200);
    } catch (ServiceException e) {
      System.out.println(e);
      System.exit(500);
    }
  }
}