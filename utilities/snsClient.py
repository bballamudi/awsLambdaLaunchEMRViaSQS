import boto3
import json

# Build SNS client

sns = boto3.client('sns', region_name='us-west-2')

# Send message for Email
publishSns = sns.publish(
    TopicArn='arn:aws:sns:us-west-2:<aws-acct-number>:emrFailure'
    , Message="There was an issue with the Hadoop cluster. "
              "You can view the logs by logging into the console and navigating to : "
              "Here is the current STDERR: " + ""
    , Subject="Hadoop ETL Failure")
