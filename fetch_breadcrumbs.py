#!/usr/bin/python3

import json
import requests
import os
from datetime import date, datetime

DIR = "/home/yl6/dataeng-project"

def logging(msg):
  print("[{now}] {msg}".format(now=datetime.now(), msg=msg))

with open("{dir}/project_config.json".format(dir=DIR)) as f:
  config = json.load(f)
  breadcrumb_output_dir = config['breadcrumbs_dir']
  f.close()

# BreadCrumData url
url = " http://rbi.ddns.net/getBreadCrumbData"
# Get request
r = requests.get(url)

# Parse json into dict
dictinfo = json.loads(r.text)

if not os.path.isdir(breadcrumb_output_dir):
  os.mkdir(breadcrumb_output_dir) 

with open("{dir}/{date}.json".format(dir=breadcrumb_output_dir, 
                                     date=date.today()), "w") as f:
  json.dump(dictinfo, f, indent=4)
  logging("breadcrumb data has been written to {dir}".format(dir=breadcrumb_output_dir))
  f.close()
