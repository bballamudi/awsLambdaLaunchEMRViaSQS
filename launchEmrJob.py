"""
Name: launchEmrJob.py

Purpose: Launches a Spark ETL Job upon submission of a message to the bankStatementHadoop SQS queue from an
ETL client. Moreover, the function waits for a message in an SQS queue from an ETL server,
proceeds to delete  an s3 output directory for said Spark job, and kicks of a Spark job on EMR. Upon duraiton of the job
another Lambda function is triggered based on the job's State.

Author: Pat Alwell
Software Engineer - Cloud
Email: pat.alwell@gmail.com

"""

import boto3
import logging

def lambda_handler(event, context):
    # Setup logging
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    try:

        response = str(event['Records'][0]['body'])

        # Create an SQS client
        sqs = boto3.client('sqs', region_name='us-west-2')

        # raw Payload for SQSs
        queueMessage = {
            'group': 'Sample Group',
            'project': 'Hadoop Project',
            'version': '1.0',
            'environment': 'AWS Production',
            'job': 'sampleSparkJob'
        }

        sQsPayload = str(queueMessage)

        # Get message from queue and run job
        logger.info("Message Body From SQS: " + response)
        logger.info("Message Body for Validation: " + sQsPayload)

        # Validate if message from queue is the same as our payload; if it is run our job
        if response == sQsPayload:

            # Create S3 client and delete JobOutput Folder for new Job
            s3 = boto3.client('s3')

            # List Objects in a bucket
            s3ObjectList = s3.list_objects(
                Bucket='my-sample-bucket'
                , Prefix='data/outputData/'
            )

            if 'Contents' in s3ObjectList:

                logger.info("Deleting objects from s3://my-sample-bucket/data/outputData/ " +
                            "prior to running the spark job" + str(s3ObjectList['Contents']))

                # Iterate over the keys in our response and pass them to the delete_objects() method
                for object in s3ObjectList['Contents']:
                    s3Object = object['Key']
                    logger.info("Deleting :" + str(s3Object))

                    s3.delete_objects(
                        Bucket='my-sample-bucket'
                        , Delete={
                            'Objects': [
                                {
                                    'Key': s3Object
                                }
                            ]
                        })
            else:
                logger.info("s3://my-sample-bucket/data/outputData/ directory is empty.")

            # create EMR client
            emr = boto3.client('emr', region_name='us-west-2')

            run_job = emr.run_job_flow(
                Name='sampleSparkJob',
                LogUri='s3n://aws-logs-<account-num>-us-west-2/elasticmapreduce/',
                ReleaseLabel='emr-5.18.0',
                Instances={
                    'InstanceGroups': [
                        {
                            'Name': 'Master',
                            'InstanceRole': 'MASTER',
                            'InstanceType': 'm3.xlarge',
                            'InstanceCount': 1
                        },
                        {
                            'Name': 'Slaves',
                            'InstanceRole': 'CORE',
                            'InstanceType': 'r3.4xlarge',
                            'InstanceCount': 3
                        }
                    ],
                    'KeepJobFlowAliveWhenNoSteps': False,
                    'Ec2KeyName': '',
                    'EmrManagedSlaveSecurityGroup': 'sg-957ght554s',
                    'EmrManagedMasterSecurityGroup': 'sg-957ght554s',
                    'Ec2SubnetId': 'subnet-bhkt5567',
                },
                Applications=[
                    {'Name': 'Spark'},
                    {'Name': 'Hadoop'}
                ],
                Steps=[
                    {
                        'Name': 'sampleSparkJob',
                        'ActionOnFailure': 'TERMINATE_CLUSTER',
                        'HadoopJarStep': {
                            'Properties': (),
                            'Jar': 'command-runner.jar',
                            'Args': [
                                'spark-submit',
                                '--deploy-mode', 'cluster',
                                '--class', 'com.palwell.Main',
                                's3://my-sample-bucket/sparkApps/mySampleApp-2.11-1.0.jar',
                                'yarn',
                                's3://my-sample-bucket/data/sourceData/',
                                's3://my-sample-bucket/data/outputData/'
                            ]
                        }
                    }
                ],
                VisibleToAllUsers=True,
                JobFlowRole='EMR_EC2_DefaultRole',
                ServiceRole='EMR_DefaultRole',
                Configurations=[
                    {
                        'Classification': 'emrfs-site',
                        'Properties': {
                            'fs.s3.serverSideEncryption.kms.keyId': 'arn:aws:kms:us-west-2:<acct-number>:alias/s3TDE',
                            'fs.s3.enableServerSideEncryption': 'true'
                        }
                    },
                ])

            # Capture Job/Cluster Id. These are the same in practice
            logger.info('Launched EMR Job : {}'.format(run_job))
            clusterId = str(run_job['JobFlowId'])
            logger.info("The Cluster/Job Id is : {}".format(clusterId))

            # Purge the bankStatementHadoop Queue
            # NOTE: There is no need for this as lambda will delete the message in the queue.

    except Exception as e:
        logger.error('Something went wrong: {}'.format(e))
