import boto3
from boto3.dynamodb.conditions import Key, Attr
import json
from random import randint
import requests
from requests_aws4auth import AWS4Auth
from elasticsearch import Elasticsearch, RequestsHttpConnection


QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/380014966022/restaurantQueue'


# --------------------------------- decipher message from SQS ---------------------------------

def dequeue():
    sqs = boto3.client('sqs', region_name='us-east-1')
    
    sqs_response = sqs.receive_message(
    QueueUrl = QUEUE_URL,
    AttributeNames=[
        'SentTimestamp'
    ],
    MaxNumberOfMessages=1,
    MessageAttributeNames=[
        'All'
    ],
    VisibilityTimeout = 0,
    WaitTimeSeconds = 0
    )
    
    if 'Messages' in sqs_response:
        message = sqs_response['Messages'][0]
        res_location = message['MessageAttributes']['Location']['StringValue']
        res_cuisine = message['MessageAttributes']['Cuisine']['StringValue']
        res_date = message['MessageAttributes']['DiningDate']['StringValue']
        res_time = message['MessageAttributes']['DiningTime']['StringValue']
        res_people = message['MessageAttributes']['PeopleNum']['StringValue']
        res_number = message['MessageAttributes']['PhoneNum']['StringValue']
    
        # delete message after usage
        receipt_handle = sqs_response['Messages'][0]['ReceiptHandle']
        sqs.delete_message(
            QueueUrl = QUEUE_URL,
            ReceiptHandle = receipt_handle
        )
    else:
        return ['Error while retrieving message from SQS!']
        
    info = []
    info.append(res_location)
    info.append(res_cuisine)
    info.append(res_date)
    info.append(res_time)
    info.append(res_people)
    info.append(res_number)
    return info


# --------------------------------- perform elastic search with cuisine keyword from SQS ---------------------------------

credentials = boto3.Session().get_credentials()
region = 'us-east-1'
service = 'es'
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

def rand_elastic_search(location, cuisine):
    es_endpoint = 'search-yelp-restaurant-jcwqo7mjstbw3yereil3vhezgy.us-east-1.es.amazonaws.com' 
    
    es = Elasticsearch(
        hosts = [{'host': es_endpoint, 'port': 443}],
        http_auth = awsauth,
        use_ssl = True,
        verify_certs = True,
        connection_class = RequestsHttpConnection
    )

    # Get the food category from queue message attributes.
    search_data = es.search(index="restaurants", body={
        "query": {
            "match": {
                "categories.title": cuisine
            }}})
            
    
    # Choose random among returned list
    total_num_searches = len(search_data['hits']['hits'])
    if total_num_searches == 0:
        return [f'Sorry! We do not have any data for {cuisine} in {location}.']

    rand_idx = randint(0,total_num_searches - 1)
    
    rand_business_ids = []
    while len(rand_business_ids) < 3:
        rand_business_ids.append(search_data['hits']['hits'][rand_idx]['_source']['id'])
        rand_idx += 1
        if rand_idx >= total_num_searches:
            rand_idx = 0
            
    return rand_business_ids


# --------------------------------- search DynamoDB to get restaurant info ---------------------------------

def dynamodb_search(rand_business_id):
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table_db = dynamodb.Table('YelpRestaurant')
    
    scanresult = table_db.query(KeyConditionExpression=Key('id').eq(rand_business_id))
    item = scanresult['Items'][0]

    name = item['name']
    address_list = item['address']
    address_list.pop() #take out "New York, NY, zipcode"
    address = ", ".join(address_list)
    review_count = item['review_count']
    rating = item['rating']
    
    return (name, address, review_count, rating)


# --------------------------------- send text message about the resturants ---------------------------------

def sendsns(message, number):
    sns = boto3.client('sns', region_name='us-east-1')
    response = sns.publish(
        PhoneNumber = number,
        Message = message,
        MessageStructure = 'string',
        MessageAttributes = {
            'AWS.SNS.SMS.SMSType': {
                'DataType': 'String',
                'StringValue': 'Transactional'
            }
        }
    )

    print(response)


# --------------------------------- MAIN ---------------------------------

def lambda_handler(event, context):

    # Collect message from SQS
    info = dequeue()
    location = info[0]

    if location.startswith('Error while'):
        print(location)

    else:
        cuisine = info[1]
        date = info[2]
        time = info[3]
        people = info[4]
        number = info[5]
        
        # Choose a random restaurant with the given cuisine
        rand_business_ids = rand_elastic_search(location, cuisine)
    
        if rand_business_ids[0].startswith('Sorry! We do'):
            # Send failure SNS message
            sendsns(rand_business_ids[0], '+1' + number)

        else:
            # Find detailed information about the restaurant
            message_per_rest = []
            for i in range(len(rand_business_ids)):
                name, address, review_count, rating = dynamodb_search(rand_business_ids[i])
                message_per_rest.append(f'{i + 1}. {name}, located at {address}')
            rest_message = ", ".join(message_per_rest)
            message = f'Hello! Here are my {cuisine} restaurant(shop) suggestions for {people} people, for {date} at {time}: {rest_message}. Enjoy your meal!'
    
            # Send sucess SNS message
            sendsns(message, '+1' + number)