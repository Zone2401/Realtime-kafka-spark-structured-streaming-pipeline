import requests
import uuid
from kafka import KafkaProducer
import time
import json
import logging

def get_data():
    try:
        response = requests.get(url="https://randomuser.me/api/", timeout=10) 
        if response.status_code == 200:
            raw = response.json()
            if raw.get('results') and len(raw['results']) > 0:
                return raw['results'][0] 
            else:
                logging.warning("API returned empty results")
                return None
        else:
            logging.error(f'Failed to fetch API data. Status code: {response.status_code}')
            return None
    except Exception as e:
        logging.error(f"Request error: {e}")
        return None

def transform_data(raw):
    if raw is None:
        return None
    
    try:
        data = {}
        
        data['id'] = str(uuid.uuid4()) 
        data['first_name'] = raw['name']['first']
        data['last_name'] = raw['name']['last']        
        data['gender'] = raw['gender']                  
        data['address'] = f"{raw['location']['street']['number']}, {raw['location']['street']['name']}, {raw['location']['city']}"
        data['email'] = raw['email']
        data['username'] = raw['login']['username']
        data['dob'] = raw['dob']['date']
        data['registered_date'] = raw['registered']['date']
        data['phone'] = raw['phone']
        data['picture'] = raw['picture']['medium']
        return data
    except KeyError as e:
        logging.error(f"Missing key in data: {e}")
        return None

def load_to_kafka():
    
    producer = KafkaProducer(
        bootstrap_servers=['broker:29092'], 
        max_block_ms=5000
    )
    
    end_time = time.time() + 300 

    while time.time() < end_time:
        try:
            raw = get_data()
            if raw:
                data = transform_data(raw)
                if data:
                    producer.send('users_profile', json.dumps(data).encode('utf-8'))
                    print(f"Sent: {data['username']}") 
            
            
            time.sleep(2) 
            
        except Exception as e:
            logging.error(f'An error occured: {e}')
            continue