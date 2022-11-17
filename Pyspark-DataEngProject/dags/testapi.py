import json
import requests

def json_scraper(url, file_name, *bucket):
    print("hello you have started")
    response = requests.request("GET", url)
    json_data = response.json()

    with open(file_name, 'w', encoding='utf-8') as json_file:
        json.dump(json_data, json_file, ensure_ascii=False, indent=4)
    
    print("done")

json_scraper("https://www.predictit.org/api/marketdata/all/",'predicit_market.json', 'fakebucket') 