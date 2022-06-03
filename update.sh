mvn install
aws lambda update-function-code --function-name JscToImage --zip-file fileb://./target/mmm-aws-lambda-1.0.0.jar
#aws lambda update-function-code --function-name JscLambdaFanoutDriver --zip-file fileb://./target/mmm-aws-lambda-1.0.0.jar
#aws lambda update-function-code --function-name JscLambdaFanoutWorker --zip-file fileb://./target/mmm-aws-lambda-1.0.0.jar
#aws lambda update-function-code --function-name mmm-lambda-java --zip-file fileb://./target/mmm-aws-lambda-1.0.0.jar
