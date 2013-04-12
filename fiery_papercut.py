# -*- coding: utf-8 -*-
"""

    Script to test Fiery cost accounting API working with PaperCut.

    Assumes access to a Fiery and a PaperCut server.

    PaperCut Setup
    --------------
    A PaperCut server can be setup in a few minutes by downloading the trial
    version from http://www.papercut.com/ . 

    If you do this then please set the admin password to "password" or 
    change DEFAULT_PAPERCUT_PWD to the PaperCut admin password.

    Viewing Fiery jobs in PaperCut
    -------------------------------
    The easiest way to view the Fiery jobs on PaperCut is with a web browser.

        Go to http://<papercut server address>:9191/app?service=page/AccountList
            (e.g. http://localhost:9191/app?service=page/AccountList if the server is 
            running on your computer)
        Click on the Fiery-account link  
        Click on the Job Log tab   

        There is more information and screen shots for this in  
        https://docs.google.com/a/papercut.com/document/d/1fj4v6kFj5GHI_pbHiO1sie_bysQZw5z6jv15YW_5fCg

    Notes:
    ------
    DEFAULT_* contain all default states
    Update DEFAULT_* to reflect your environment or vice-versa

    TODO:
        Handle multiple Fierys
        Enable page level color detection for Fiery printer
        Remove exit()s in production code
"""
from __future__ import division

import requests
import json
import re
import sys
import optparse
import time
import datetime
import xmlrpclib
import pprint
import cPickle as pickle

#
# Utility functions
#

PRETTY_PRINTER =  pprint.PrettyPrinter(indent=4)

def pprint(obj):
    """Pretty print object obj"""
    PRETTY_PRINTER.pprint(obj)

def save_object(path, obj):
    """Save obj to path"""
    pickle.dump(obj, open(path, 'wb'))    
    
def load_object(path, default=None):
    """Load object from path"""
    try:
        return pickle.load(open(path, 'rb'))
    except:
        return default  

def first_non_empty(iterable):
    """Return first non-empty element in iterable"""
    return next(s for s in iterable if s)   
    
#
# Fiery code
#        

def fiery_load_api_key(key_file):   
    try:   
        return open(key_file, 'rb').read()
    except Exception, e:
        print('Could not load Fiery API key from %s: %s' % (key_file, e))
        return None


def fiery_login(api_key, fiery, username, password, verbose=False): 
    """Login to Fiery and return a URL and session cookie
        TODO: 
            Don't exit()
    """

    auth = {
        'username': username, 
        'password': password,
        'accessrights': {'a1': api_key}
    }
    if verbose:
        print('fiery_login: fiery=%s,auth=%s'  
               % (fiery, json.dumps(auth)))

    if not fiery:
        print('error: no server specified')
        exit(2)
    
    url = 'https://%s/live' % fiery
    
    r = requests.post('https://%s/live/login' % fiery, 
                      data=json.dumps(auth), 
                      headers={'content-type': 'application/json'}, 
                      verify=False)
    if r.status_code != 200:
        print('error: http code ' + str(r.status_code))
        exit(6)
    
    print('Connected to Fiery %s' % url)

    # Check for cookies
    if 'set-cookie' not in r.headers:
        print('error: No cookies')
        exit(7)

    # Extract login coookie
    cookies = r.headers['set-cookie']
    m = re.search('_session_id=([^ ;]+)', cookies)
    if not m:
        print('error: No session cookie')
        exit(8)
    sessionCookie = m.group(1)
    if verbose:
        print('sessionCookie=%s' % sessionCookie)

    return url, sessionCookie    

def fiery_fetch_jobs(url, sessionCookie, start_id, count, verbose=False): 

    # Request job log

    headers = {'Cookie': '_session_id=%s;' % sessionCookie}
    full_url = '%s/api/v1/cost?start_id=%d&count=%d' % (url, start_id, count)

    if verbose:
        print('Retrieving Fiery jobs: url="%s"' % full_url)

    r = requests.get(full_url, headers=headers, verify=False)
    if r.status_code != 200:
        print('error: url=%s, http code=%d' % (full_url, r.status_code))
        exit(9)

    # The list of printed jobs
    return json.loads(r.text)


#
# PaperCut code
# 

def papercut_init(host_name, port, auth_token, account_name):
    """Standard PaperCut XMLRPC initialization
        Instantiate a server and create the accout if it doesn't 
        already exist.
        
        host_name: Network name/IP address of PaperCut server
        port: PaperCut server port to use
        auth_token: Authorization token or password
        account_name: Name of PaperCut Shared Account to be used to store Fiery jobs
        Returns: Instantiated XMLRPC server
    """

    print('Connecting to PaperCut "%s:%d"' % (host_name, port))

    server = xmlrpclib.Server('http://%s:%d/rpc/api/xmlrpc' % (host_name, port))

    if server.api.isSharedAccountExists(auth_token, account_name):
        print('PaperCut Shared Account "%s" account already exists.' % account_name)
    else:
        # user does not exist so create
        print('Creating PaperCut Shared Account "%s"' % account_name)
        server.api.addNewSharedAccount(auth_token, account_name)

    return server    

def papercut_log_jobs(server, auth_token, pc_jobs):
    """Record Fiery jobs in PaperCut job log
        
        server: PaperCut XMLRPC server instance
        auth_token: Authorization token or password
        pc_jobs: dict of jobs to record in PaperCut job log (in PaperCut format)
    """
    for job in pc_jobs.values():
        # PaperCut doesn't log non-printing print jobs
        if job['total-pages'] <= 0:
            continue
        # See http://www.papercut.com/products/ng/manual/ "Importing Print Job Details"    
        job_details = ','.join('%s=%s' % (k,v) for k,v in job.items())
        print job_details
        server.api.processJob(auth_token, job_details)    


#
# Job conversion/manipulation code
#
def convert_boolean(fy_bool_str):
    if fy_bool_str:
        if fy_bool_str[0].lower() == 'y':
            return 'TRUE'
        if fy_bool_str[0].lower() == 'n':
            return 'FALSE'
    return '???'  
    
def convert_time(fy_time):
    """Convert a Fiery date-time to a PaperCut date-time

        fy_time: Fiery date-time
        Returns: PaperCut date-time corresponding to fy_time if fy_time 
            is parsed successfully or an obviously bogus date-time if
            it is not.

        TODO: 
            Is Fiery date-time GMT or local?
            Is Fiery date-time string localized or always English?
    """ 
    try:
        tm = time.strptime(fy_time, '%H:%M %b %d, %Y')  
        dt = datetime.datetime(*tm[:6])
        return dt.isoformat().replace(':', '').replace('-', '')
    except e:
        print('convert_time: Invalid fy_time="%s": e' % (fy_time, e))
        return '1111-11-11T11:11:11'


  
FIERY_PAPERCUT_MAP = {
    'printer': lambda job: job['fiery'],
    'user': lambda job: first_non_empty([job.get('username', None), 
                            job.get('authuser', None), 'blank on Fiery']),
    'comment': lambda job: 'Fiery id: %s' % job['id'],
    'time': lambda job: convert_time(job['date']),
    'document-name': lambda job: job['title'],
    'document-size-kb': lambda job: int(job['size'])//1024,
    'paper-size-name': lambda job: job['media size'],
    'copies': lambda job: int(job['copies printed']),
    'total-pages': lambda job: (int(job['total blank pages printed'])
                              + int(job['total bw pages printed'])
                              + int(job['total color pages printed'])),
    'total-color-pages': lambda job: int(job['total color pages printed']),
    'duplex': lambda job: convert_boolean(job['duplex printed']),
}
   
def convert_job(fy_job, pc_server, pc_account_name):
    """Convert a Fiery job to a PaperCut job

        fy_job: Fiery job as a dict
        pc_account_name: PaperCut account name
        Returns: PaperCut job corresponding to fy_job

        TODO: Check this conversion with Fiery team
    """    
    pc_job = dict((k, FIERY_PAPERCUT_MAP[k](fy_job)) for k in FIERY_PAPERCUT_MAP) 
    pc_job['server'] = pc_server
    pc_job['shared-account'] = pc_account_name
    pc_job['grayscale'] = 'TRUE' if  pc_job['total-color-pages'] else 'FALSE'
    return pc_job 


#
# Command line processing
#    
DEFAULT_FIERY_API_KEY_FILE = 'fiery.api.key'   
DEFAULT_FIERY_IP = '192.68.228.104'   
DEFAULT_FIERY_USER = 'admin' 
DEFAULT_FIERY_PWD = 'Fiery.color' 
DEFAULT_FIERY_BATCH_SIZE = 1000 

DEFAULT_PAPERCUT_IP = 'localhost'
DEFAULT_PAPERCUT_PORT = 9191
DEFAULT_PAPERCUT_PWD = 'password'
DEFAULT_PAPERCUT_ACCOUNT = 'Fiery-account'

DEFAULT_SLEEP_SECS = 60

parser = optparse.OptionParser('python %s [options]' % sys.argv[0])
parser.add_option('-B', '--fiery-batch-size', dest='fiery_batch_size', default=DEFAULT_FIERY_BATCH_SIZE, 
        help='Number of jobs to request in each call to Fiery')
parser.add_option('-K', '--fiery-api-key', dest='fiery_api_key_file', default=DEFAULT_FIERY_API_KEY_FILE, 
        help='Path of Fiery API key file')        
parser.add_option('-S', '--fiery-ip', dest='fiery_ip', default=DEFAULT_FIERY_IP, 
        help='Network name or IP address of Fiery to monitor')
parser.add_option('-U', '--fiery-user', dest='fiery_user', default=DEFAULT_FIERY_USER, 
        help='Fiery username')
parser.add_option('-P', '--fiery-pwd', dest='fiery_pwd', default=DEFAULT_FIERY_PWD, 
        help='Fiery password')
parser.add_option('-s', '--papercut-ip', dest='papercut_ip', default=DEFAULT_PAPERCUT_IP,
        help='Network name or IP address of PaperCut server') 
parser.add_option('-o', '--papercut-port', dest='papercut_port', type='int', default=DEFAULT_PAPERCUT_PORT, 
        help='PaperCut admin password')
parser.add_option('-p', '--papercut-pwd', dest='papercut_pwd', default=DEFAULT_PAPERCUT_PWD, 
        help='PaperCut admin password')
parser.add_option('-a', '--papercut-account', dest='papercut_account', default=DEFAULT_PAPERCUT_ACCOUNT, 
        help='Name of PaperCut shared account to log Fiery prints in')
parser.add_option('-v', '--verbose', action='store_true', dest='verbose', default=False, 
        help='verbose output')     
parser.add_option('-t', '--sleep-secs', dest='sleep_secs', type='int', default=DEFAULT_SLEEP_SECS, 
        help='Sleep time between successive Fiery polls')    
parser.add_option('-i', '--ignore-history', action='store_true', dest='ignore_history', default=False, 
        help='Ignore history of Fiery jobs logged on PaperCut and log all jobs again.')        

#
# Execution starts here
#
options,args = parser.parse_args()

# 
# We can't check command line params as they are optional (i.e. args is empty) 
# so we just print them to stdout.
#  
print('-' * 80)
print(__doc__)
print('     --help for more information')
print('-' * 80)
print('Options')    
pprint(options.__dict__)
print('-' * 80)

# Initialize Fiery connection  
fy_api_key = fiery_load_api_key(options.fiery_api_key_file)
if not fy_api_key:
    exit()

#if options.verbose:
#    print(options.fiery_api_key_file)   
#    print('Fiery API Key="%s"' % fy_api_key) 
fy_url,fy_session_cookie = fiery_login(fy_api_key, options.fiery_ip, 
        options.fiery_user, options.fiery_pwd, options.verbose) 

# Initialize PaperCut        
pc_ip = options.papercut_ip
pc_auth_token = options.papercut_pwd
pc_account = options.papercut_account
pc_server = papercut_init(options.papercut_ip, options.papercut_port,
        pc_auth_token, options.papercut_account)   

# Load record of Fiery jobs already logged on PaperCut
# TODO: Store this in PaperCut in some way  
fy_last_ids = {} if options.ignore_history else load_object('fiery.last.ids', {}) 
    
if options.verbose:
    print('Latest job ids from previous session: %s' % fy_last_ids) 
fy_start_id = fy_last_ids.get(options.fiery_ip, 0)   

fy_count = options.fiery_batch_size   

print('=' * 80)    

# Main loop
#   Poll Fiery for list of printed jobs
#   Convert jobs to PaperCut format
#   Compare to record of jobs already logged in PaperCut
#   If there are any new jobs   
#       Log new jobs
#       Update list record of jobs already logged
while True:  
 
    # Fetch Fiery jobs
    fy_jobs = fiery_fetch_jobs(fy_url, fy_session_cookie, fy_start_id, fy_count, options.verbose)
    
    if fy_jobs:
        print('Fetched %d jobs from %s' % (len(fy_jobs), fy_url))
        if options.verbose:
            print(fy_jobs) 

        # Convert Fiery jobs to PaperCut format
        pc_jobs = dict((job['id'],convert_job(job, pc_ip, pc_account)) for job in fy_jobs)
        if options.verbose:
            print(pc_jobs) 

        # Log latest Fiery on PaperCut
        papercut_log_jobs(pc_server, pc_auth_token, pc_jobs) 

        # Update record of jobs already logged
        max_id = max(pc_jobs.keys())
        assert max_id >= fy_start_id, 'Highest job id=%d < start id=%d' % (max_id, fy_start_id)

        fy_start_id = max_id + 1
        fy_last_ids[options.fiery_ip] = fy_start_id   
        save_object('fiery.last.ids', fy_last_ids)   

    if options.verbose:
        print('Sleeping %d sec' % options.sleep_secs)
        print('-' * 80)  
    time.sleep(options.sleep_secs)
