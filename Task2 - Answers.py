import os
import json
import argparse
import subprocess
import pandas as pd
from os import listdir
from filecmp import cmp
from pathlib import Path
from datetime import datetime
from os.path import isfile, join
from urllib.parse import urlparse
from subprocess import PIPE, Popen

parser = argparse.ArgumentParser()

parser.add_argument("path",
                    help="Enter directory path")

parser.add_argument("trgtpath",
                    help="Enter target directory path")
                             
parser.add_argument("-u", "--unix", action="store_true", dest="unix", default=False,
                    help="Maintain UNIX time stamp")

args = parser.parse_args()

# list for all files in a directory
files = [item for item in listdir(args.path) 
         if isfile(join(args.path, item))]

trgtfiles = [itm for itm in listdir(args.trgtpath)
	     if isfile(join(args.trgtpath, itm))]
####################################
for item in files:
    begin_time = datetime.now()
    records = [json.loads(line) for line in open(join(args.path, item))]

    #browser #os
    a = [d['a'] for d in records if 'a' in d]
    #x=a[1].split('/', maxsplit=1)[0]

    browser = []
    os1 =[]
    OS = []
    for i in a:
        browser.append(i.split('/', maxsplit=1)[0])
        os1.append(i.split(';',maxsplit=1)[0])
     


    for i in os1:
        OS.append(i.partition('(')[2])
    
    #time_zone
    #city
    time_zone = [d['tz'] for d in records if 'tz' in d]
    city = list(map(lambda d: d.get('cy', " "), records))

    #from_urls 
    #to_urls

    fr_url = [d['r'] for d in records if 'r' in d]
    t_url = [d['u'] for d in records  if 'u' in d]

    fr_url = list(map(urlparse,fr_url))
    t_url = list(map(urlparse,t_url))

    from_urls =[]
    for i in fr_url:
        from_urls.append(i.netloc)
        #print(attr)

    to_urls =[]
    for i in t_url:
        to_urls.append(i.netloc)


    #time_in #time_out
    if args.unix:
        time_in = [d['t'] for d in records  if 't' in d]
        time_out = [d['hc'] for d in records if 'hc' in d]
    else:
        time_in = list(map(datetime.fromtimestamp,[d['t'] for d in records if 't' in d]))
        time_out = list(map(datetime.fromtimestamp,[d['hc'] for d in records if 'hc' in d]))
    
    
    #longitude
    #latitude
    ll = list(map(lambda d: d.get('ll',"  "), records))
    longitude , latitude = map(list, zip(*ll)) 
    #print(longitude , "\n",latitude )

    df = pd.DataFrame(list(zip(browser, OS , time_zone, city,from_urls ,to_urls,time_in , time_out,longitude, latitude)), 
    columns = ['browser', 'os' , 'time_zone', 'city','from_urls' ,'to_urls','time_in', 'time_out','longitude', 'latitude'])
    pth = Path(f"transformed_file_{item}.csv")
    df.to_csv( join(args.trgtpath,pth))
    print("Path of the file: " ,args.path)
    print("Execution Time: " , datetime.now() - begin_time)
    print("# of transformed rows: ", len(df))
    print("#######################")

# list all documents
DATA_DIR = Path(args.trgtpath)
files = sorted(os.listdir(DATA_DIR))

# list containing the classes of documents with the same content
duplicates = []

# comparison of the documents
for file in files:

    is_duplicate = False

    for class_ in duplicates:
        is_duplicate = cmp(
            DATA_DIR / file,
            DATA_DIR / class_[0],
            shallow = False
        )
        if is_duplicate:
            class_.append(file)
            break
    if not is_duplicate:
        duplicates.append([file])     

# show results
print ("list of duplicated files: " ,duplicates)



