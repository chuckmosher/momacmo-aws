aws lambda create-function --function-name jsc-lambda --zip-file fileb:///Users/chuck/eclipse-workspace/AWS_Lambda/target/jsc-lambda-1.0.0.jar --handler org.momacmo.aws.lambda.jscio.JscLambdaHandler --runtime java8.al2 --role arn:aws:iam::694027992080:role/lambda-sqs-role