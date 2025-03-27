for i in range(1,11):
    print(f"{i}*2=i*2")
print("I have changed here")
print("I am branch of the git")
print("How is it?")

import sys
import os
from datetime import datetime
from django.shortcuts import render
from django.conf import settings
from SkinProductA.pushToElasticsearch import PushDataToES
from SkinProductA.customer_details import CustomerDetails
import redis
from kafka import KafkaProducer
import json

producer=KafkaProducer( bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"))
# Update the path to allow imports from parent directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Helper function for Elasticsearch connection
def get_es_connection():
    host = settings.ES_HOST  # You should define these in your settings.py
    
    
    connection = PushDataToES(host)
    if not connection.es.ping():
        raise Exception("Elasticsearch is not serviceable. Please try again later")
    return connection

# View to show data from Elasticsearch
def show_data(request):
    time_result = None
    error_msg = None
    result = None

    try:
        es_conn = get_es_connection()
        start_time = datetime.now()
        result = es_conn.available_data_es()
        end_time = datetime.now()
        time_result = end_time - start_time
    except Exception as e:
        error_msg = str(e)

    data_list = {
        "output": result,
        "time": time_result,
        "error_msg": error_msg
    }
    return render(request, "show_data.html", data_list)


def homepage(request):
    return render(request, "homepage.html")


def load_data(request):
    time_taken = None
    error_msg = None
    result = None

    try:
        es_conn = get_es_connection()
        start_time = datetime.now()
        result = es_conn.available_data_es()
        end_time = datetime.now()
        time_taken = end_time - start_time
    except Exception as e:
        error_msg = str(e)

    data_list = {
        "result": result,
        "time_taken": time_taken,
        "error_msg": error_msg
    }
    return render(request, "load.html", data_list)

def search_data_page(request):
    result = None
    search_data = ""
    error_msg = None
    time_taken = None

    try:
        if request.method == "POST":
            search_data = request.POST.get("search_data", "").strip()
            if not search_data:
                return render(request, "homepage.html")  # No search input, redirect to homepage

            r=redis.Redis(host="localhost", port=6379, decode_responses=True)
            start_time = datetime.now()
            cached_result = r.hget("search_data", search_data)
            end_time = datetime.now()
            time_taken = end_time - start_time
            if cached_result:
                result = eval(cached_result)  # Convert string back to dictionary/list
                print(" Data retrieved from Redis cache.")
            else:
                
                cust_obj = CustomerDetails(settings.ES_HOST)
                if not cust_obj.es.ping():
                    raise Exception("Elasticsearch is not serviceable. Please try again later")

                start_time = datetime.now()
                result = cust_obj.search_data(search_data)
                
                end_time = datetime.now()
                time_taken = end_time - start_time

                r.hset("search_data", search_data, str(result))
                r.expire("search_data", 600)  # Expire cache in 10 minutes

                print(" Data retrieved from Elasticsearch and stored in Redis.")

                # Step 5: Store Search Frequency in Sorted Set
            r.zincrby("search:queries", 1, search_data)

            print(" Search term frequency updated in Redis.")
            kafka_message = {"query": search_data, "result": result, "timestamp": str(datetime.now())}
            producer.send("searchdata", kafka_message)
            print(f" Search result sent to Kafka: {kafka_message}")
    except Exception as e:
        error_msg = str(e)

    search_list = {
        "result": result,
        "time_taken": time_taken,
        "error_msg": error_msg,
        "search_data": search_data
    }
    return render(request, "search.html", search_list)
