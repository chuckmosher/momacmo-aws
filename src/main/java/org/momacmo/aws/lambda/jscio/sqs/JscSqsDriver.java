package org.momacmo.aws.lambda.jscio.sqs;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.AmazonSQSException;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.ListQueuesResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;

public class JscSqsDriver {
  public static void main(String[] args)
  {
      final AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();


      String queueUrl = sqs.getQueueUrl("jsc-io-queue").getQueueUrl();
      ListQueuesResult lq_result = sqs.listQueues();
      System.out.println("Your SQS Queue URLs:");
      for (String url : lq_result.getQueueUrls()) {
          System.out.println(url);
      }
      System.out.println("Send message ...");
      SendMessageRequest send_msg_request = new SendMessageRequest()
              .withQueueUrl(queueUrl)
              .withMessageBody("{\"bucket\": \"momacmos3\",\"prefix\": \"momacmo/meagerdas/1432_aws_output_filt_5_50_despike\", \"nsamp\": \"1250\",\"ntrace\": \"380\",\"volume\": \"11\",\"frame\": \"403\"}")
              .withDelaySeconds(1);
      SendMessageResult result = sqs.sendMessage(send_msg_request);
      System.out.println("Result:");
      System.out.println(result);

      sqs.shutdown();
      // Send multiple messages to the queue
      /*
      SendMessageBatchRequest send_batch_request = new SendMessageBatchRequest()
              .withQueueUrl(queueUrl)
              .withEntries(
                      new SendMessageBatchRequestEntry(
                              "msg_1", "Hello from message 1"),
                      new SendMessageBatchRequestEntry(
                              "msg_2", "Hello from message 2")
                              .withDelaySeconds(10));
      sqs.sendMessageBatch(send_batch_request);
      */
  }
}
