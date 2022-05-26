import json
import boto3
from botocore.exceptions import ClientError
import os

# you can create environmental variables in lambda->configuration from the console
ACCESS_KEY = os.environ['access_key']
SECRET_KEY = os.environ['secret_key']
QUEUE_URL = os.environ['queue_url']


def get_phone_number_from_S3(BUCKET, KEY):
    """
    :param BUCKET:  Bucket name we got through the post request.
    :param KEY:     Key name we got through the post request.
    :return:        The phone number extracted from the S3 file.
    """

    try:
        S3 = boto3.client(
            's3',
            region_name = 'us-west-1',
            aws_access_key_id = ACCESS_KEY,
            aws_secret_access_key = SECRET_KEY
        )
        #Create a file object using the bucket and object key.
        fileobj = S3.get_object(
            Bucket=BUCKET,
            Key= KEY
            )
        # open the file object and read it into the variable filedata.
        fileData = fileobj['Body'].read()
        return fileData.decode('UTF-8')
    except ClientError as error:
        print(error)
        raise(error)



def upload_file_to_sqs(QUEUE_URL, uploadData):
    """
    :param QUEUE_URL: Task Queue URL
    :param uploadData: Data to be used by the dispatcher lambda
    :return  no return
    """
    sqs = boto3.client('sqs',region_name = "us-west-1",
                               aws_access_key_id=ACCESS_KEY,
                               aws_secret_access_key=SECRET_KEY)
    try:
        sqs.send_message(QueueUrl = QUEUE_URL, MessageBody = uploadData)
        print("Message uploaded to the queue")
    except ClientError as error:
        print("there is an error in  sqs function ")
        raise(error)

def lambda_handler(event, context):
    """
    on sucessfull completion of the code we get a message that the message posted in the sqs queue
    """
    try:
        # extract the message passed through post request body
        data_string = event['body']
        # convert the string to dictionary to access them
        data = json.loads(data_string)

        # Post request objects
        ACTION = data['Action']
        BUCKET_NAME = data['Bucket']
        KEY = data['Key']
        CAMPAIGN = data['Campaign']

        # extract the phone number from
        PHONE_NUMBER = get_phone_number_from_S3(BUCKET_NAME, KEY)
        SQS_MESSAGE = {"phoneNumber": PHONE_NUMBER, "command": ACTION , "campaign": CAMPAIGN}

        upload_file_to_sqs(QUEUE_URL, str(SQS_MESSAGE))

        return {
            'statusCode': 200,
            'body': json.dumps("The file is uploaded to the sqs queue!")
        }
    except ClientError as err:
        return {
            'statusCode': 424,
            'body': json.dumps("XXXXXXXX There is a problem in the code! XXXXXXXXX" + "\n The error is :" + str(err))
        }
