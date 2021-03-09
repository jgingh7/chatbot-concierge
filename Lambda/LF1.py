import json
import os
import math
import dateutil.parser
import datetime
import time
import logging
import boto3


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def get_slots(intent_request):
    return intent_request['currentIntent']['slots']

def close(session_attributes, fulfillment_state, message):
    response = {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }
    
    return response
    
def build_validation_result(is_valid, violated_slot, message_content):
    if message_content is None:
        return {
            "isValid": is_valid,
            "violatedSlot": violated_slot
        }

    return {
        'isValid': is_valid,
        'violatedSlot': violated_slot,
        'message': {'contentType': 'PlainText', 'content': message_content}
    }
    
def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            'message': message
        }
    }

def isvalid_date(date):
    try:
        dateutil.parser.parse(date)
        return True
    except ValueError:
        return False

def parse_int(n):
    try:
        return int(n)
    except ValueError:
        return float('nan')

def delegate(session_attributes, slots):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': slots
        }
    }


# --------------------------------- other intents ---------------------------------

def GreetingIntent(intent_request):
    
    return {
        'dialogAction': {
            "type": "ElicitIntent",
            'message': {
                'contentType': 'PlainText', 
                'content': 'Hi there, how can I help?'}
        }
    } 

def ThankYouIntent(intent_request):
    return  {
        'dialogAction': {
            "type": "ElicitIntent",
            'message': {
                'contentType': 'PlainText', 
                'content': 'You are welcome.'}
        }
    } 


# --------------------------------- perfom validation ---------------------------------

def validateIntentSlots(location, cuisine, num_people, date, given_time, phone_num):

    locations = ['new york', 'manhattan']
    if location is not None and location.lower() not in locations:
        return build_validation_result(False,
                                      'location',
                                      'Sorry! We do not serve recommendations for this location right now!')
                                   
    cuisines = ['korean', 'chinese', 'coffee', 'american', 'indian', 'japanese']
    if cuisine is not None and cuisine.lower() not in cuisines:
        return build_validation_result(False,
                                      'cuisine',
                                      'Sorry! We do not serve recommendations for this cuisine right now!')
                                       
    if num_people is not None:
        num_people = int(num_people)
        if num_people > 20 or num_people <= 0:
            return build_validation_result(False,
                                      'num_people',
                                      'Sorry! Number of people should be at least 1 and at most 20!')
    
    if date:
        # invalid date
        if not isvalid_date(date):
            return build_validation_result(False, 'date', 'I did not understand that, what date would you like to add?')
        # user entered a date before today
        elif datetime.datetime.strptime(date, '%Y-%m-%d').date() < datetime.date.today():
            return build_validation_result(False, 'date', 'You can search restaurant from today onwards. What day would you like to search?')

    if given_time:
        hour, minute = given_time.split(':')
        hour = parse_int(hour)
        minute = parse_int(minute)
        if math.isnan(hour) or math.isnan(minute):
            # Not a valid time; use a prompt defined on the build-time model.
            return build_validation_result(False, 'given_time', 'Not a valid time')

    
    if phone_num and len(phone_num) != 10:
        return build_validation_result(
            False,
            'phone_num',
            'Sorry, {} is not a valid phone number. Please provide a valid US phone number.'.format(phone_num)
        )
    
    return build_validation_result(True, None, None)


# --------------------------------- validate input and send to SQS ---------------------------------

def dining_suggestion_intent(intent_request):
    location = get_slots(intent_request)["location"]
    cuisine = get_slots(intent_request)["cuisine"]
    num_people = get_slots(intent_request)["num_people"]
    date = get_slots(intent_request)["date"]
    given_time = get_slots(intent_request)["given_time"]
    phone_num = get_slots(intent_request)["phone_num"]
    session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}

    requestData = {
                    "cuisine": cuisine,
                    "location":location,
                    "peopleNum": num_people,
                    "date": date,
                    "time": given_time,
                    "phoneNum": phone_num
                }
                
    print (requestData)

    session_attributes['requestData'] = json.dumps(requestData)
    

    if intent_request['invocationSource'] == 'DialogCodeHook':
        slots = get_slots(intent_request)
        
        # validate inputs
        validation_result = validateIntentSlots(location, cuisine, num_people, date, given_time, phone_num)
        
        # If validation fails, elicit the slot again 
        if not validation_result['isValid']:
            slots[validation_result['violatedSlot']] = None
            print ("elicit slot")
            return elicit_slot(session_attributes,
                              intent_request['currentIntent']['name'],
                              slots,
                              validation_result['violatedSlot'],
                              validation_result['message'])
        return delegate(session_attributes, intent_request['currentIntent']['slots'])
    
    messageId = sendSQSMessage(requestData)
    print (messageId)

    return close(intent_request['sessionAttributes'],
             'Fulfilled',
             {'contentType': 'PlainText',
              'content': 'Got all the data, You will receive recommendation soon.'})


# --------------------------------- send info to SQS ---------------------------------

def sendSQSMessage(requestData):
    
    sqs = boto3.client('sqs')
    queue_url = 'https://sqs.us-east-1.amazonaws.com/380014966022/restaurantQueue'
    
    messageAttributes = {
        'Cuisine': {
            'DataType': 'String',
            'StringValue': requestData['cuisine']
        },
        'Location': {
            'DataType': 'String',
            'StringValue': requestData['location']
        },
        "DiningTime": {
            'DataType': "String",
            'StringValue': requestData['time']
        },
        "DiningDate": {
            'DataType': "String",
            'StringValue': requestData['date']
        },
        'PeopleNum': {
            'DataType': 'Number',
            'StringValue': requestData['peopleNum']
        },
        'PhoneNum': {
            'DataType': 'String',
            'StringValue': requestData['phoneNum']
        }
    }
    
    messageBody=('Slots for the Restaurant')
    print (messageBody)
    
    response = sqs.send_message(
        QueueUrl = queue_url,
        DelaySeconds = 2,
        MessageAttributes = messageAttributes,
        MessageBody = messageBody
        )
    print (response)
    
    return response['MessageId']
    
    
# --------------------------------- dispatch according to the intent ---------------------------------

def dispatch(intent_request):
    intent_name = intent_request['currentIntent']['name']

    # dispatch according to the intent
    if intent_name == 'GreetingIntent':
        return GreetingIntent(intent_request)
    elif intent_name == 'DiningSuggestionsIntent':
        return dining_suggestion_intent(intent_request)
    elif intent_name == 'ThankYouIntent':
        return ThankYouIntent(intent_request)

    raise Exception('Intent with name ' + intent_name + ' not supported')
    

# --------------------------------- MAIN ---------------------------------

def lambda_handler(event, context):
    os.environ['TZ'] = 'America/New_York'
    time.tzset()
    
    return dispatch(event)