import boto3
import mimetypes
import json

# Initialize clients for S3 and SNS
s3_client = boto3.client('s3')
sns_client = boto3.client('sns')

# SNS Topic ARN
SNS_TOPIC_ARN = "arn:aws:sns:your-region:your-account-id:your-topic-name"

def lambda_handler(event, context):
    try:
        # Extract bucket name and object key from the S3 event
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        file_key = event['Records'][0]['s3']['object']['key']
        
        # Fetch metadata for the object
        response = s3_client.head_object(Bucket=bucket_name, Key=file_key)
        
        # Extract metadata details
        file_name = file_key
        file_size = response['ContentLength']  # in bytes
        content_type = response.get('ContentType', mimetypes.guess_type(file_key)[0] or 'Unknown')
        
        # Log metadata
        print(f"File Name: {file_name}")
        print(f"File Size: {file_size} bytes")
        print(f"Content Type: {content_type}")
        
        # Prepare email message
        email_message = f"""
        A new file has been uploaded to S3:
        
        Bucket Name: {bucket_name}
        File Name: {file_name}
        File Size: {file_size} bytes
        Content Type: {content_type}
        """
        
        # Publish message to SNS topic
        sns_response = sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject="New S3 File Notification",
            Message=email_message
        )
        
        # Log SNS response
        print(f"SNS Message ID: {sns_response['MessageId']}")
        
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Notification sent successfully!",
                "sns_message_id": sns_response['MessageId']
            })
        }
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
