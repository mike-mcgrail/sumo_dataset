#####
# Source: https://github.com/SumoLogic/sumologic-python-sdk/blob/main/scripts/search-job-messages.py
# Functions: 
#   1. Query SumoLogic API and return JSON, with optional timebased checkpointing -OR- parse JSON passed to script
#   2. Create DataSet payload
#   3. [optional] Send JSON to DataSet API
#
# Python3 requirements:
#   1. https://pypi.org/project/sumologic-sdk/
#   2. https://pypi.org/project/requests/ 
#   3. https://pypi.org/project/python-dateutil/
#
# Setup:
#   1. ensure script has write access to current directory to create time checkpoint file
#   2. pip3 install requirements.txt
#   3. Update sumoConfig, sumoSearch, datasetConfig below to match environment (see TODO: comments below)
#
# Usage:
#      python3 sumo_dataset.py sumo <sumo || json> --checkpoint <no || yes> --payload <json string> --dataset <no || yes>
#                              /\                  /\                       /\                      /\
#                              |                   |                        |                       |
#                              required            optional,default=no,   optional,default unused   optional,default=yes
#                                                  start=midnight,
#                                                  end=run time
#
# Examples:
#   1. Query SumoLogic and send results to DataSet:
#      python3 sumo_dataset.py sumo
#   2. Query SumoLogic, update checkpoint (helpful for recurring cron job) and send to DataSet: (note: if checkpoint is not set, times default to earlist=midnight latest=execution time)
#      python3 sumo_dataset.py --checkpoint yes
#   3. Instead of SumoLogic, pass JSON to script:
#      python3 sumo_dataset.py json --payload '{ "firstName": "mike", "lastName": "mcgrail" }'
#   4. Provide json directly and view payload without sending to DataSet:
#      python3 sumo_dataset.py json --payload '{ "firstName": "mike", "lastName": "mcgrail" }' --dataset no
#
# Considerations:
#   1. sumo logic was defined for a known set of data with results returned with | json, may need adjustment
#   2. DataSet addEvetns API has a payload limit of 6MB. This needs to be considered for SumoLogic query LIMIT number of records
#   3. Using sumo + checkppoint cannot work with future timestamps since now() is used as end
#   4. JSON sources likely need to be tested with edge cases
#####

import json
import os
import sys
import time
import requests
import datetime
import dateutil.parser
import argparse
from sumologic import SumoLogic

sumoConfig = {
    "accessId": "<string>", #TODO: replace <string> with accessId
    "accessKey": "<string>", #TODO: replace <string> with accessKey
    "endpoint": "https://api.ca.sumologic.com/api" #TODO: adjust SumoLogic locale if needed
}

sumoSearch = {
    "q": '_sourceCategory=*| json field=_raw "host"', #TODO: update SumoLogic query as needed, script expects | json results
    "timeZone": "EST", #TODO: update timeZone as needed
    "byReceiptTime": "false",
    "delay": 5, #TODO: adjust delay as needed
    "LIMIT": 50 #TODO: adjust number of results to return. DataSet addEvents API has a limit of 
}

datasetConfig = {
    "endpoint": "https://app.scalyr.com/api/addEvents", #TODO: adjust DataSet locale if needed
    "token": "<string>" #TODO: replace <string> with DataSet write API key
}


def sumo_search_messages(sumo_start, sumo_end):
    sumo = SumoLogic(sumoConfig["accessId"], sumoConfig["accessKey"], sumoConfig["endpoint"])
    start_time = epoch_to_timestamp(sumo_start)
    end_time = epoch_to_timestamp(sumo_end)
    sj = sumo.search_job(sumoSearch["q"], start_time, end_time, sumoSearch["timeZone"], sumoSearch["byReceiptTime"])
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


def dataset_create_payload(arg_source):
    ds_event_dict = {}
    if arg_source == "sumo":
        ds_event_dict["session"] = "sumoLogic"
        ds_event_dict["sessionInfo"] = {"serverHost": "sumoLogic"}
    else:
        ds_event_dict["session"] = "json"
        ds_event_dict["sessionInfo"] = {"serverHost": "script"}
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
    ts = timestamp_to_epoch(str(raw["timestamp"]))
    timestamp = ts * 1000000000
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


def timestamp_to_epoch(timestamp):
    parsed_time = dateutil.parser.parse(timestamp)
    return int(parsed_time.timestamp())


def epoch_to_timestamp(epoch):
    dt = datetime.datetime.fromtimestamp(epoch)  
    return(dt.strftime( "%Y-%m-%dT%H:%M:%S"))


def write_time_file(sumo_times):
    f = open(os.path.join(os.path.dirname(__file__), 'sumo_times.txt'),  "w")
    f.write(json.dumps(sumo_times))
    f.close()
    return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('source', help="sumo or json", choices=['sumo','json'], type=str.lower) # Required argument <sumo> or <json>
    # Optional arguments
    parser.add_argument('--checkpoint', default='no', choices=['yes','no'], help="if source is sumo, update sumo_times checkpoint file (default is no)", type=str.lower)
    parser.add_argument('--payload', help="if source is json, this is the payload", type=str)
    parser.add_argument('--dataset', default='yes', choices=['yes','no'], help="send data to dataset", type=str.lower)

    args = parser.parse_args()

    if args.source == 'sumo':
        try: # Read sumo_times.txt, which is in format { "start": <epoch>, "end": <epoch> }
            f = open(os.path.join(os.path.dirname(__file__), 'sumo_times.txt'),  "r")
            f_data = f.read()
            f.close()
            sumo_times = json.loads(f_data)
        except: # If file doesn't exist, default times to local time midnight and now and create it
            sumo_times = {}
            sumo_times["start"] = timestamp_to_epoch(str(datetime.date.today()))
            sumo_times["end"] = timestamp_to_epoch(str(datetime.datetime.now()))
            write_time_file(sumo_times)

        sumoResults = sumo_search_messages(sumo_times["start"], sumo_times["end"])
        if 'messages' in sumoResults:
            messages = sumoResults['messages']
            if "map" in messages[0]:
                ds_event_dict = dataset_create_payload(args.source)

                for i in range(len(messages)):
                    raw = messages[i]["map"]["_raw"]
                    raw = json.loads(raw)

                    ts = timestamp_to_epoch(str(raw["timestamp"]))
                    if args.checkpoint == 'yes': # If updating checkpoint
                        if ts > sumo_times["start"]:
                            sumo_times["start"] = ts # Set largest start time
                            sumo_times["start"] = timestamp_to_epoch(str(datetime.date.today())) # Set current time as end time
                            write_time_file(sumo_times)
                    
                    dataset_update_payload(ds_event_dict, raw) # Add message to payload
                
                if args.dataset == 'yes':
                    ds_result = dataset_send_payload(ds_event_dict)
                    print(ds_result)
                else:
                    print("NOT SENDING TO DATASET. Payload is:")
                    print(json.dumps(ds_event_dict))

    elif args.source == 'json':
        try:
            payload = json.loads(args.payload)
            ds_event_dict = dataset_create_payload(args.source)
            ds_event_dict["events"].append(
                {
                    "log": "1",
                    "attrs": {
                     "message": payload,
                    }
                }
            )
            if args.dataset == 'yes':
                ds_result = dataset_send_payload(ds_event_dict)
                print(ds_result)
            else:
                print("NOT SENDING TO DATASET. Payload is:")
                print(json.dumps(ds_event_dict))

        except:
            print("unable to parse json")

if __name__ == "__main__":
    main()
