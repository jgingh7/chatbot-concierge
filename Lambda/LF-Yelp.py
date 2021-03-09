import json
import boto3
import datetime
import requests
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

API_KEY = '######################SECRET######################################'
ENDPOINT = 'https://api.yelp.com/v3/businesses/search'
CUISINES = ['korean restaurant', 'chinese restaurant', 'coffee', 'american restaurant', 'indian restaurant', 'japanese restaurant']

LOCATION = 'manhattan'
SEARCH_RADIUS = 40000
YELP_LIMIT = 50

# --------------------------------- MAIN ---------------------------------

def lambda_handler(event, context):
    resultData = []
    
    for cuisine in CUISINES: #need to run just for 2 cuisines for each execution, since lambda has execution time limit
        for i in range(20):
            requestData = {
                        'term': cuisine,
                        'limit': YELP_LIMIT,
                        'radius': SEARCH_RADIUS,
                        'offset': 50 * i,
                        'location': LOCATION
                    }

            headers = {
                'Authorization': 'Bearer %s' % API_KEY,
            }
                
            response = requests.get(ENDPOINT, headers=headers, params=requestData)
            message = json.loads(response.text)
            result = message['businesses']
            resultData.append(result)
    
    # Add data to DynamodDB
    dynamoInsert(resultData)
    
    # Add index data to the ElasticSearch
    addElasticIndex(resultData)   
        
    return {
        'statusCode': 200,
        'body': json.dumps('success')
    }


# --------------------------------- insert restaurant info to DynamoDB ---------------------------------

def dynamoInsert(restaurants):
    
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('YelpRestaurant')
    
    for search_block in restaurants:
        for restaurant in search_block:
        
            tableEntry = {
                'id': restaurant['id'],
                'name': restaurant['name'],
                'categories': restaurant['categories'],
                'rating': int(restaurant['rating']),
                'review_count': int(restaurant['review_count']),
                'address': restaurant['location']['display_address']
            }        
    
            if (restaurant['coordinates'] and restaurant['coordinates']['latitude'] and restaurant['coordinates']['longitude']):
                tableEntry['latitude'] = str(restaurant['coordinates']['latitude'])
                tableEntry['longitude'] = str(restaurant['coordinates']['longitude'])
    
            if (restaurant['location']['zip_code']):
                tableEntry['zip_code'] = restaurant['location']['zip_code']
    
            # Add necessary attributes to the yelp-restaurants table
            table.put_item(
                Item={
                    'insertedAtTimestamp': str(datetime.datetime.now()),
                    'id': tableEntry['id'],
                    'name': tableEntry['name'],
                    'address': tableEntry['address'],
                    'latitude': tableEntry.get('latitude', None),
                    'longitude': tableEntry.get('longitude', None),
                    'review_count': tableEntry['review_count'],
                    'rating': tableEntry['rating'],
                    'zip_code': tableEntry.get('zip_code', None),
                    'categories': tableEntry['categories']
                   }
                )


# --------------------------------- insert indicies to Elastic Search ---------------------------------

# Add elastic search indices after DB has been added

credentials = boto3.Session().get_credentials()
region = 'us-east-1'
service = 'es'
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

def addElasticIndex(restaurants):
    host = 'search-yelp-restaurant-jcwqo7mjstbw3yereil3vhezgy.us-east-1.es.amazonaws.com' 
    
    es = Elasticsearch(
        hosts = [{'host': host, 'port': 443}],
        http_auth = awsauth,
        use_ssl = True,
        verify_certs = True,
        connection_class = RequestsHttpConnection
    )
    
    for search_block in restaurants:
        for restaurant in search_block:
        
            index_data = {
                'id': restaurant['id'],
                'categories': restaurant['categories']
            }                            

            es.index(index="restaurants", doc_type="Restaurant", id=restaurant['id'], body=index_data, refresh=True)