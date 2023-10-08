import os
import time
from jugaad_data.nse import NSELive
import requests
import pymongo
mongo_db_connection='mongoDb address port'
base_url = "otto bot alert server address"
frequency_update = 100
try:
    print("going to start the process")
    t1 = time.time()
    client = pymongo.MongoClient(mongo_db_connection)
    all_databases = client.list_database_names()
    db = client["ottoBot"]
    collection = db["StockAvailable"]
    StockAlert = db["StockAlert"]
    time.sleep(100)
    n = NSELive()

    while True:#use time condition here
        t2 = time.time()
        if(t2-t1>frequency_update):
            t1 = time.time()
            a = n.live_fno()['data']
            new_price = []
            b=[]
            for i in a:
                new_price.append({'stock':i['symbol'], "price":i['lastPrice']})

            # Create a list of update operations using bulk write
            bulk_updates = [
                pymongo.UpdateOne(
                    {"stock": stock_info['stock']},
                    {"$set": {"price": float(stock_info['price'])}}
                )
                for stock_info in new_price
            ]
            # Execute bulk updates
            result = collection.bulk_write(bulk_updates, ordered=False)#ordered=False option allows the updates to continue even if some fail.


                # Check the result of the bulk write
            if result.modified_count > 0:
                print(f"Updated {result.modified_count} stock prices")
            else:
                print("No stock prices were updated")
            print('\n\n')
            query = {"subscriber": {"$gt": 0}}
            stocks_with_subscribers = collection.find(query)
            stock_list = list(stocks_with_subscribers)

            # Filter the documents from the StockAlert collection
            filter_criteria = {
                "status": "pending",
                "stock": {"$in": [stock_info['stock'] for stock_info in stock_list]},
                "price": {"$in": [float(stock_info['price']) for stock_info in stock_list]}
            }

             # Find documents in the StockAlert collection that match the filter criteria
            matching_alerts = StockAlert.aggregate([
            {
                "$match": filter_criteria
            },
            {
                "$lookup": {
                    "from": "User",  # Replace "User" with the actual name of the user collection
                    "localField": "user",
                    "foreignField": "_id",
                    "as": "user_info"
                }
            },
            {
                "$unwind": "$user_info"
            },
            {
                "$project": {
                    "userId":"$user_info._id",
                    "userName": "$user_info.name",
                    "token": "$user_info.pushToken",
                    "alertDetails": {
                        "status": "$status",
                        "stock": "$stock",
                        "price": "$price"
                    }
                }
                }
            ])

            # Print the resulting list of matching alerts
            # for alert in matching_alerts:
            #     print(alert)
            for alert in matching_alerts:
                try:
                    user_name = alert["userName"]
                    stock_name = alert["alertDetails"]["stock"]
                    price = alert["alertDetails"]["price"]
                    token = alert["token"]
                    alertId = str(alert['_id'])
                    userId = str(alert['userId'])

                    # Define the request payload
                    payload = {
                        "name": user_name,
                        "stock": stock_name,
                        "price": price,
                        "pushToken": token,
                        "userId": userId, 
                        "alertId": alertId
                        
                        
                    }
                    print(payload)
                    # Send a POST request to your local server
                    response = requests.post(f"{base_url}/v1/alert/trigger", json=payload)

                    # Check the response
                    if response.status_code == 201:
                        print(f"Push Request sent successfully for user: {user_name}")
                        print(response.json())
                    else:
                        print(f"Push Request Failed to send request for user: {user_name}")
                        print(response.json())
                except Exception as e:
                    print(e)
            time.sleep(100)
            print("\n\n##############################################################\n")
        
except Exception as e:
    client.close()
    print(e)
    os.system(f"python stockLtp1.py")
