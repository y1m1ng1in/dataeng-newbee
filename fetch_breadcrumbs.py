#!/usr/bin/python3

import json
import requests
import os
from datetime import date, datetime
from time import sleep

DIR = "/home/yl6/dataeng-project"
# Max number of times to retrying request url
MAX_RETRY = 10
# Second between two requests
RETRY_DELAY = 2

def logging(msg):
  print("[{now}] {msg}".format(now=datetime.now(), msg=msg))

def get_data(url, retry_max, delay):
  for _ in range(retry_max):
    try:
      r = requests.get(url)
      if r:
        return r
    except requests.exceptions.ConnectionError as e:
      logging("requests.exceptions.ConnectionError, retry..")
      sleep(delay)
    except requests.exceptions.RequestException as e:
      logging("requests.exceptions.RequestException, retry..")
      sleep(delay)
    except requests.exceptions.ProxyError as e:
      logging("requests.exceptions.ProxyError, retry..")
      sleep(delay)
    
  logging("cannot make request to {}".format(url))  

with open("{dir}/project_config.json".format(dir=DIR)) as f:
  config = json.load(f)
  breadcrumb_output_dir = config['breadcrumbs_dir']
  f.close()

# BreadCrumData url
url = "http://rbi.ddns.net/getBreadCrumbData"
# Get request
r = get_data(url, 10, 2)
# Parse json into dict
dictinfo = json.loads(r.text)

if not os.path.isdir(breadcrumb_output_dir):
  os.mkdir(breadcrumb_output_dir) 

with open("{dir}/{date}.json".format(dir=breadcrumb_output_dir, 
                                     date=date.today()), "w") as f:
  json.dump(dictinfo, f, indent=4)
  logging("breadcrumb data has been written to {dir}".format(dir=breadcrumb_output_dir))
  f.close()
