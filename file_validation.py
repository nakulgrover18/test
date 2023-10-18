import boto3
import json
import pandas as pd

s3_client = boto3.client('s3')


def lambda_handler(event, context):

    print(event)
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']


    download_path = '/tmp/{}'.format(key)
    s3_client.download_file(bucket, key, download_path)


    df = pd.read_excel(download_path)

    expected_columns = ["Id", "Name", "Address"]
    actual_columns = df.columns.tolist()

    if set(expected_columns) != set(actual_columns):
        # Send SNS notification
        sns_message = f"File {key} is not proper. Expected columns: {expected_columns}, Actual columns: {actual_columns}"
        sns_client.publish(TopicArn='arn:aws:sns:us-east-1:865935148049:key-reminder', Message=sns_message)
        print('Email Send')
    else:
        # Send SQS message
        sqs_message = {'file_name': key}
        sqs_client.send_message(QueueUrl='https://sqs.us-east-1.amazonaws.com/865935148049/test', MessageBody=json.dumps(sqs_message))
        print('Message publish to ')

    return {
        'statusCode': 200,
        'body': json.dumps('File processing complete.')
    }
