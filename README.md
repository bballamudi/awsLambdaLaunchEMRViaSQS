# awsLambdaLaunchEMRViaSQS

<h3>Synopsis:</h3>

<p>These python 3.7 applications automate the deployment of a data warehouse ETL job with  AWS lambda and various event triggers.</p>

<p>The utilities folder contains test event data and clients that mock various AWS API requests like deleting files, sending notifications via SNS, and sending notifications to Slack in regards to application state.
The folder also contains two test events for use in Lambda. The sqsEventEmrLaunch.json API event corresponds to launchEmrJob.py while emrStepStateChange.json API event corresponds to logEMRJob.py.</p>

<h3>Lambda Functions:</h3>
<p>
<b>launchEmrJob.py</b> 
Launches a Spark ETL Job upon submission of a message to an SQS queue from an ETL client. Moreover, the function waits for a message in an SQS queue from an ETL server, proceeds to delete the s3 output directory for a Spark job, and kicks of a Spark job on EMR. During runtime, another Lambda function is triggered based on the jobs State.
</p>

<p>
<b>logEMRJob.py</b>
This function is the second function in our ETL process. Upon initiation of an EMR
job this function collects step state from a cloudwatch event e.g. aws.emr and manages the outcome.
Failures are sent to an email via SNS while job success triggers a snowflake (datwarehouse) job via an SQS message from an ETL server.
</p>

<h3>Logging and Debugging:</h3>
<p>
Users can debug the applications by visiting CloudWatch in the AWS console or by polling the appropriate log groups and streams. In this case CloudWatch -> LogGroups -> `/aws/lambda/aunchEmrJob and /aws/lambda/logEMRJob`
</p>

