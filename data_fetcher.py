import json
import requests
import os
import re
from datetime import date
from urllib.request import urlopen
from bs4 import BeautifulSoup


class DataFetcher:
    
    def __init__(self, url, retry_max, delay, logging):
        self.url = url
        self.retry_max = retry_max
        self.delay = delay
        self.logging = logging
        self.response = self.__get_data()
        self.dict_entries = []
        
    def __get_data(self):
        for _ in range(self.retry_max):
            try:
                r = requests.get(self.url)
                if r:
                    return r
            except requests.exceptions.ConnectionError as e:
                logging("requests.exceptions.ConnectionError, retry..")
                sleep(self.delay)
            except requests.exceptions.RequestException as e:
                logging("requests.exceptions.RequestException, retry..")
                sleep(self.delay)
            except requests.exceptions.ProxyError as e:
                logging("requests.exceptions.ProxyError, retry..")
                sleep(self.delay)

        self.logging("cannot make request to {}".format(self.url))
        
    def write(self, dirname=".", filename=None, datatype="breadcrumb"):
        if not os.path.isdir(dirname):
            os.mkdir(dirname)
        if not filename:
            filename = str(date.today())
        with open("{dir}/{file}.json".format(dir=dirname, file=filename), "w") as f:
            json.dump(self.dict_entries, f, indent=4)
            self.logging("{datatype} data has been written to {dir}".format(
                dir=dirname, datatype=datatype))
            f.close()

            
class BreadCrumbFetcher(DataFetcher):
    
    def __init__(self, retry_max=10, delay=2, logging=lambda x:x):
        pass # not refactored yet

        
class StopEventFetcher(DataFetcher):
    
    def __init__(self, retry_max=10, delay=2, logging=lambda x:x):
        super().__init__(
            "http://rbi.ddns.net/getStopEvents", retry_max, delay, logging)
        self.ptn_trip_id = r'Stop Events for trip (\d{9}) for today'
        self.prog_trip_id = re.compile(self.ptn_trip_id)
        self.soup = BeautifulSoup(self.response.text, 'html.parser')
        
    def parse(self):
        self.dict_entries = []
        
        first_one = self.soup.find('h1') # the first header of the webpage
        first_header = first_one.next_sibling # the first header of the first table
        first_table = first_header.next_sibling # the first table
        
        # the rest of the tables in the website should have the same header as 
        # the first one
        self.headers = [h for h in first_table.find('tr').stripped_strings]
        
        header = first_header
        table = first_table
        # iterate all the tables in the webpage, each row becomes a dict object
        while header and table:
            trip_id = self.prog_trip_id.match(header.string).group(1) # get trip id
            table_rows = table.find_all('tr')[1:] # get all rows of current table
            for r in table_rows:
                self.dict_entries.append({
                    'trip_id': trip_id, 
                    **dict(zip(self.headers, [d.string for d in r.find_all('td')]))
                })
            header = table.next_sibling
            if header:
                table = header.next_sibling


if __name__ == '__main__':
    DIR = "/home/yl6/dataeng-project"
    with open("{dir}/project_config.json".format(dir=DIR)) as f:
        config = json.load(f)
        event_stop_output_dir = config['event_stop_dir']
        f.close()
    fetcher = StopEventFetcher()
    fetcher.parse()
    fetcher.write(dirname=event_stop_output_dir, datatype="event stop")