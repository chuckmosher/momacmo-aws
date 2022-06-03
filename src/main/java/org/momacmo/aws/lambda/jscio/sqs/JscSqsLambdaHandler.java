package org.momacmo.aws.lambda.jscio.sqs;

import org.javaseis.compress.TraceCompressor;
import org.javaseis.properties.DataFormat;
import org.momacmo.aws.lambda.jscio.demo.JscLambdaFanoutWorker;
import org.momacmo.aws.lambda.jscio.demo.JscLambdaInput;
import org.momacmo.aws.lambda.jscio.demo.JscLambdaOutput;
import org.momacmo.aws.s3.jscio.properties.JsonUtil;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

public class JscSqsLambdaHandler implements RequestHandler<SQSEvent, String> {
  @Override
  public String handleRequest(SQSEvent event, Context context) {
    AmazonS3 s3 = null;
    LambdaLogger logger = context.getLogger();
    try {
      logger.log("Get S3 client ... ");
      System.out.flush();
      s3 = AmazonS3ClientBuilder.standard().build();
      logger.log("... S3 client found");
    } catch (Exception e) {
      e.printStackTrace();
      throw new IllegalStateException("Failure: Could not connect to S3\n"+e.getMessage(),e.getCause());
    }
    JscLambdaInput input = null;
    byte[] trcBytes = null;
    for (SQSMessage msg : event.getRecords()) {
      String body = msg.getBody();
      input = (JscLambdaInput) JsonUtil.fromJsonString(JscLambdaInput.class, body);
      System.out.println(JsonUtil.toJsonString(input));
      if (trcBytes == null) {
        int reclen = 0;
        try {
          reclen = TraceCompressor.getRecordLength(DataFormat.COMPRESSED_INT16, input.nsamp);
        } catch (Exception e) {
          e.printStackTrace();
          throw new IllegalStateException("Failure: Could not connect to S3\n"+e.getMessage(),e.getCause());
        }
        if (input.ntrace == 0 || reclen == 0) {
          throw new IllegalStateException("Failure: record length is zero");
        }
        trcBytes = new byte[input.ntrace * reclen];
      }
      //JscLambdaOutput output = JscProcessingWorker.getFrameTraces(s3, input, trcBytes);
      //outputList.add(output);
    }

    return "OogaBooga";
  }

  public static void main(String[] args) {
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
    input.setRange( 403, 11 );
    input.setNsamp(1250);
    input.setNtrace(380);
    byte[] trcBytes = new byte[380 * 1250];
    JscLambdaOutput output =  JscLambdaFanoutWorker.getFrameTraces(s3, input, input.frm0, input.vol0, trcBytes);
    System.out.println(JsonUtil.toJsonString(output));
  }
}