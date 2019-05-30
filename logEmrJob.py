"""
Name: logEmrJob.py

Purpose: This function is the second function to automating our ETL events with Spark and EMR. Upon initiation of an EMR
job this function collects step state from a cloudwatch event e.g. aws.emr and manages the outcome.
Failures are sent to an email via SNS while job success triggers a snowflake job via an SQS message to an
ETL server.

Author: Pat Alwell
Email: pat.alwell@gmail.com
Software Engineer - Cloud
"""
import boto3
import logging
import json
from io import BytesIO
from gzip import GzipFile


def lambda_handler(event, context):
    # Setup logging
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    try:

        # get relevent event data
        response = str(event)
        name = str(event['detail']['name'])
        clusterId = str(event['detail']['clusterId'])
        stepState = str(event['detail']['state'])
        stepId = str(event['detail']['stepId'])

        # If our step has the name of our job run the logic
        if name == 'sampleSparkJob':

            logger.info("Logging the aws.emr event : {}".format(event))
            logger.info("Found a step with name: {} and clusterId: {}".format(name, clusterId))

            # If our job was successful
            if stepState == 'COMPLETED':
                logger.info(
                    "The sampleSparkJob was successful. A message will be sent to the ETL SQS queue.")

                rawMessage = {"group": "techLeads", "project": "ETL", "version": "default",
                              "environment": "Snowflake Prod", "job": "Spark_Post_Map"}
                messageForETL = json.dumps(rawMessage)
                sqs = boto3.client('sqs', region_name='us-west-2')

                # Send payload to MatillionQueue
                request = sqs.send_message(QueueUrl="<sample-SQS-QueueUrl>",
                                           MessageBody=str(messageForETL))

            # If our job failed
            elif stepState == 'FAILED':
                logger.info("The sampleSparkJob failed. An email will be sent about the failure.")

                # create an s3 client
                s3 = boto3.client('s3')

                """
                We need to get our logObject of type botocore.response.StreamingBody, convert it to Bytes,
                and decompress the GZipped payload.

                References: https://gist.github.com/veselosky/9427faa38cee75cd8e27
                https://botocore.amazonaws.com/v1/documentation/api/latest/reference/response.html
                """

                s3Object = s3.get_object(
                    Bucket='aws-logs-<acct-number>-us-west-2'
                    , Key='elasticmapreduce/{}/steps/{}/stderr.gz'.format(clusterId, stepId))

                # Cast the StreamingBodyBuffer to Bytes
                rawByteStream = BytesIO(s3Object['Body'].read())

                # Unzip the byteArray and decode to utf-8
                rawText = GzipFile(None, 'rb', fileobj=rawByteStream).read().decode('utf-8')

                # send an email about the failure
                sns = boto3.client('sns', region_name='us-west-2')

                sns.publish(
                    TopicArn='arn:aws:sns:us-west-2:<account-number>:emrFailure'
                    , Message="There was an issue with the Hadoop cluster. "
                              "You can view the logs by logging into the console and navigating to "
                              "s3://aws-logs-<account-number>-us-west-2/elasticmapreduce/{}/steps/{}/stderr.gz "
                              "\nHere is the current STDERR : \n {}".format(clusterId, stepId, rawText)
                    , Subject="Bank Statement Hadoop ETL Failure")

    except Exception as e:
        logger.error('Something went wrong: {}'.format(e))
