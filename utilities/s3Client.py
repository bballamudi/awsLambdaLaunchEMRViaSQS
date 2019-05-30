import boto3

# Create S3 client and delete JobOutput Folder for new Job
s3 = boto3.client('s3')

# List Objects in a bucket
s3ObjectList = s3.list_objects(
    Bucket='my-sample-bucket'
    , Prefix='data/outputData/'
)

print(s3ObjectList)
if 'Contents' in  s3ObjectList:

    print("Getting objects from s3://my-sample-bucket/data/outputData/\n" + str(s3ObjectList['Contents']))

    # Iterate over the keys in our response and pass them to the delete_objects() method
    for object in s3ObjectList['Contents']:
        s3Object = object['Key']
        print("Deleting :" + str(s3Object))

        s3.delete_objects(
            Bucket='my-sample-bucket'
            , Delete={
                'Objects': [
                    {
                        'Key': s3Object
                    }
                ]
            })
