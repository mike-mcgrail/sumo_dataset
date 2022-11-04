#####
# Author: Mike McGrail
# Date: 4 November, 2022
# Function: Query SumoLogic API, retrieve JSON, send to DataSet API
# Python3 requirements:
#   1. https://pypi.org/project/sumologic-sdk/
#   2. https://pypi.org/project/requests/ 
#   3. https://pypi.org/project/python-dateutil/
# Instructions: Set SumoLogic config and search parameters and DataSet config
#####

import json
import sys
import time
import requests
import dateutil.parser
from sumologic import SumoLogic

sumoConfig = {
    "accessId": "",
    "accessKey": "",
    "endpoint": "https://api.ca.sumologic.com/api"
}

sumoSearch = {
    "q": '_sourceCategory=*| json field=_raw "host"',
    "fromTime": "2022-11-03T04:00:00",
    "toTime": "2022-11-03T04:30:00",
    "timeZone": "EST",
    "byReceiptTime": "false",
    "delay": 5,
    "LIMIT": 100
}

datasetConfig = {
    "endpoint": "https://app.scalyr.com/api/addEvents",
    "token": ""
}


def sumo_search_messages():
    sumo = SumoLogic(sumoConfig["accessId"], sumoConfig["accessKey"], sumoConfig["endpoint"])
    sj = sumo.search_job(sumoSearch["q"], sumoSearch["fromTime"], sumoSearch["toTime"], sumoSearch["timeZone"], sumoSearch["byReceiptTime"])
    status = sumo.search_job_status(sj)
    while status['state'] != 'DONE GATHERING RESULTS':
        if status['state'] == 'CANCELLED':
            break
        time.sleep(sumoSearch["delay"])
        status = sumo.search_job_status(sj)

    print(status['state'])

    if status['state'] == 'DONE GATHERING RESULTS':
        count = status['messageCount']
        limit = count if count < sumoSearch["LIMIT"] and count != 0 else sumoSearch["LIMIT"] # compensate bad limit check
        r = sumo.search_job_messages(sj, limit=limit)
        return(r)


def dataset_create_payload():
    ds_event_dict = {}
    ds_event_dict["session"] = "sumoLogic"
    ds_event_dict["sessionInfo"] = {"serverHost": "sumoLogic"}
    ds_event_dict["events"] = []
    ds_event_dict["logs"] = []
    ds_event_dict["logs"].append(
        {
            "id": "1",
            "attrs": {
                "parser": "json"
            }
        }
    )
    return ds_event_dict 


def dataset_update_payload(ds_event_dict, raw):
    # Convert ISO date time top epoch
    timestamp = str(raw["timestamp"])
    parsed_time = dateutil.parser.parse(timestamp)
    timestamp = parsed_time.timestamp() * 1000000000
    ds_event_dict["events"].append(
        {
            "log": "1",
            "ts": str(int(timestamp)),
            "attrs": {
                "message": raw,
            }
        }
    )
    return None


def dataset_send_payload(ds_event_dict):
    ds_headers = { "Authorization": "Bearer " + datasetConfig["token"] }
    ds_payload = json.loads(json.dumps(ds_event_dict))
    print(json.dumps(ds_event_dict))
    r = requests.post(url=datasetConfig["endpoint"], json=ds_payload, headers=ds_headers)
    if r.ok:
        return("Successfully sent to DataSet")
    else:
        return(r.text)


def main():
    sumoResults = sumo_search_messages()
    if 'messages' in sumoResults:
        messages = sumoResults['messages']
        if "map" in messages[0]:
            ds_event_dict = dataset_create_payload()
            for i in range(len(messages)):
                raw = messages[i]["map"]["_raw"]
                raw = json.loads(raw)
                dataset_update_payload(ds_event_dict, raw)
            
            ds_result = dataset_send_payload(ds_event_dict)
            print(ds_result)


if __name__ == "__main__":
    main()
