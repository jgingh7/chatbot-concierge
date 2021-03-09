import json
import boto3


def lambda_handler(event, context):
    client = boto3.client('lex-runtime')
    
    user_message = json.loads(event['body'])['messages'][0]['unstructured']['text']
    user_id = event['requestContext']['accountId']
    lex_bot = 'MyMyChatBot'
    bot_alias = 'mymyChatAlias'
        
    response = client.post_text(
        botName = lex_bot,
        botAlias = bot_alias,
        userId = user_id,
        inputText = user_message
    )   
    
    
    formatted_response = {
            'messages' :  [{
                'type' : 'unstructured',
                'unstructured' : {
                    'id' : response['ResponseMetadata']['RequestId'],
                    'text': response['message'],
                    'timestamp' : response['ResponseMetadata']['HTTPHeaders']['date']
                }
            }]
        }
    
    
    return {
        'statusCode': 200,
        'headers': { 
            'Access-Control-Allow-Headers' : 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        'body': json.dumps(formatted_response)
    }