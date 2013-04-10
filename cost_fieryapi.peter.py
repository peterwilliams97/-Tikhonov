# cost_client.py
# Use Puthon v3 to run this program
#

import requests
import json
import re
import sys

verbose = True
reqStart = 0
reqCount = 100
dumpJSON = True

# Login
API_key = \
'Bqa3GPwei+NU9z79dZskruGgJHQK53CJvXXsnaKNcy13ji9ol7Dpl0UmkUYr' \
'n7a9bP3OAtvSUpAnh1LRzi5oGXMFZVWkzsIAvD9cApALEugQOEd8a4QTSUrX' \
'01GYIEA1V3tnZ8+hfeBjTvaF7ie3FzmVec3RsIFknWaK0UeztY8sjWTi9hnK' \
'5+XxBQ6mE3Zkgaz4q4b2aeNDiJitvUuhy1Pxbs0HVx8FBlbvB59CTURUIWpT' \
'fZPvsiDhG+mBy8GyAUEfIcuLa1Ua/z5bIIMzrJZzvaMp97ufNjImcY8VsHvE' \
'aVtwW6bfaaihOphoCtk35EodSHGIbTNPmJJfws3L4K1u/J6koc7vlV7iAVtH' \
'PNn6m86bg37plf2RpAuHFOEZiLDa4XkkpdjHU/ZUK88tOt57zn/WU0csfFNb' \
'sRlrSJUDLiFWufb8m62r2D9b75/XDZKS8bAthJ0vV6y/DW7uulJ8h5q3iSrJ' \
'l9eIm2ORGRAhATqph2kbSV86FmknW6ZO'

_auth = {
    'username': 'admin', 
    'password': 'Fiery.color',
    'accessrights': {'a1': API_key}
}

fileName = 'costoutput.txt'
fiery = "fieryapi.efi.com"
fiery = '192.68.228.104'


def fetch_fiery_jobs(fiery, username='admin', password='Fiery.color'): 

    auth = {
        'username': username, 
        'password': password,
        'accessrights': {'a1': API_key}
    }
    if verbose:
        print json.dumps(auth)

    if not fiery:
        print('error: no server specified')
        exit(2)
    
    if verbose:
        print('connecting to ' + fiery + '...')

    url = 'https://%s/live' % fiery
    print url
   
    r = requests.post(url + '/login', data=json.dumps(auth), 
                      headers={'content-type': 'application/json'}, 
                      verify=False)
    if r.status_code != 200:
        print('error: http code ' + str(r.status_code))
        exit(6)
    if verbose:
        print('connected')

    # Check for cookies
    if 'set-cookie' not in r.headers:
        print('error: No cookies')
        exit(7)

    # Extract login coookie
    cookies = r.headers['set-cookie']
    m = re.search('_session_id=([^ ;]+)', cookies)
    if not m:
        print("error: No session cookie")
        exit(8)
    sessionCookie = m.group(1)
    if verbose:
        print('sessionCookie=%s' % sessionCookie)

    # Request job log
    if verbose:
        print("retrieving...");
    headers = {'Cookie': '_session_id=%s;' % sessionCookie}
    full_url =  '%s/api/v1/cost?start_id=%d&count=%d' % (url, reqStart, reqCount)
    r = requests.get(full_url, headers=headers, verify=False)
    if r.status_code != 200:
        print('error: http code ' + str(r.status_code))
        exit(9)

    # Print the list of printed jobs
    prnJobs = json.loads(r.text)
    if verbose:
        print(str(len(prnJobs)) + ' entries retrieved')
    
    return prnJobs

prnJobs = fetch_fiery_jobs(fiery)    
# Is JSON output requested?
if dumpJSON:

    jsonDump = json.dumps(prnJobs, sort_keys = False, indent = 2);
    if len(fileName) != 0:
        txtFile = open(fileName, 'w')
        txtFile.write(jsonDump + '\n')
    else:
        print(jsonDump);
    

if len(fileName) != 0:
  if verbose:
    print('stored in "' + fileName + '"');
