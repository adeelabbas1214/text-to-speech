import boto3
import json
import uuid

dynamodb = boto3.resource('dynamodb')
polly = boto3.client('polly')
s3 = boto3.client('s3')

def lambda_handler(event, context):
    try:
        # Ensure that 'Records' exists in the event
        message = json.loads(event['Records'][0]['Sns']['Message'])
    except KeyError as e:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'Invalid event structure', 'missing_key': str(e)})
        }

    post_id = message.get('PostId')
    
    if not post_id:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'PostId not found in the message'})
        }
    
    # Retrieve the post from DynamoDB
    table = dynamodb.Table('Posts')
    response = table.get_item(Key={'PostId': post_id})
    
    if 'Item' not in response:
        return {
            'statusCode': 404,
            'body': json.dumps({'error': 'Post not found'})
        }

    text = response['Item']['Text']
    
    # Convert the text to speech using Polly
    polly_response = polly.synthesize_speech(
        Text=text,
        OutputFormat='mp3',
        VoiceId='Joanna'
    )
    
    # Store the MP3 in S3
    s3.put_object(
        Bucket='tts-mp3-files',
        Key=f'{post_id}.mp3',
        Body=polly_response['AudioStream'].read(),
        ContentType='audio/mpeg'
    )
    
    # Update DynamoDB with MP3 URL
    mp3_url = f'https://{s3.meta.endpoint_url}/tts-mp3-files/{post_id}.mp3'
    table.update_item(
        Key={'PostId': post_id},
        UpdateExpression="SET MP3Url = :url, Status = :status",
        ExpressionAttributeValues={
            ':url': mp3_url,
            ':status': 'Completed'
        }
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'TTS completed', 'MP3Url': mp3_url})
    }
