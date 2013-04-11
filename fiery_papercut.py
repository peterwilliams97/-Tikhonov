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
    
    You should run this script on the computer the PaperCut server is running on.

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


    DEFAULT_* contain all default states
    Update DEFAULT_* to reflect your environment or vice-versa

    TODO:
        Handle multiple Fierys
        Figure out how to request only jobs printed on Fiery since last request
        Scan through all jobs on Fiery (see fy_reqStart, fy_reqCount) 
        Enable page level color detection for Fiery printer
        Remove exit()s in production code
        Add more Fiery job attributes to PaperCut jobs
        Fix PaperCut server\printer name for Fierys
"""
from __future__ import division

import requests
import json
import re
import sys
import optparse
import time
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


#
# Fiery code
#        
        
fy_reqStart = 0
fy_reqCount = 100

def fiery_load_api_key(key_file):   
    """Load Fiery API key from key_file and return it."""
    try:   
        return open(key_file, 'rb').read()
    except Exception, e:
        print('Could not load Fiery API key from %s: %s' % (key_file, e))
        return None


def fiery_login(api_key, fiery, username, password, verbose=False): 

    auth = {
        'username': username, 
        'password': password,
        'accessrights': {'a1': api_key}
    }
    if verbose:
        print json.dumps(auth)

    if not fiery:
        print('error: no server specified')
        exit(2)

    if verbose:
        print('connecting to ' + fiery + '...')

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

def fiery_fetch_jobs(url, sessionCookie, verbose=False): 

    # Request Fiery job log
    if verbose:
        print("retrieving...");
    headers = {'Cookie': '_session_id=%s;' % sessionCookie}
    full_url = '%s/api/v1/cost?start_id=%d&count=%d' % (url, fy_reqStart, fy_reqCount)
    r = requests.get(full_url, headers=headers, verify=False)
    if r.status_code != 200:
        print('error: url=%s, http code=%d' % (full_url, r.status_code))
        exit(9)

    # Print the list of printed jobs
    prnJobs = json.loads(r.text)
    if verbose:
        print(str(len(prnJobs)) + ' entries retrieved')

    return prnJobs

#
# PaperCut code
# 

def papercut_init(host_name, port, auth_token, account_name):
    """Standard PaperCut XMLRPC initialization
        Instantiate a server and create a shared account if it doesn't 
        already exist.
        
        host_name: Network name/IP address of PaperCut server
        port: PaperCut server port to use
        auth_token: Authorization token or password
        account_name: Name of PaperCut Shared Account to be used to store Fiery jobs
        Returns: Instantiated XMLRPC server
    """

    print('Connecting to PaperCut host %s:%d' % (host_name, port))

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
  
FIERY_PAPERCUT_MAP = {
    'comment': lambda job: 'Fiery id: %s' % job['id'],
    'user': lambda job: job['username'] if job['username'] else 'blank on Fiery',
    'server': lambda job: job['fiery'],
    'printer': lambda job: 'print',
    'copies': lambda job: int(job['copies printed']),
    'total-pages': lambda job: (int(job['total blank pages printed'])
                              + int(job['total bw pages printed'])
                              + int(job['total color pages printed'])),                              
    'total-color-pages': lambda job: int(job['total color pages printed'])
}
   
def job_fiery_to_papercut(fy_job, pc_account_name):
    """Convert a Fiery job to a PaperCut job

        fy_job: Fiery job as a dict
        pc_account_name: PaperCut account name
        Returns: PaperCut job corresponding to fy_job

        TODO: 
            Check this conversion with Fiery team
            Add remaining attributes
    """    
    pc_job = dict((k, FIERY_PAPERCUT_MAP[k](fy_job)) for k in FIERY_PAPERCUT_MAP) 
    pc_job['shared-account'] = pc_account_name
    return pc_job 

def get_new_jobs(old_pc_jobs, pc_jobs):   
    """Return dict of PaperCut jobs that in pc_jobs (latest jobs found 
        on Fiery) but not in old_pc_jobs (Fiery jobs recored in 
        PaperCut)
        
        old_pc_jobs: dict of Fiery jobs recorded in PaperCut
        pc_jobs: dict of latest jobs found on Fiery
        Returns: dict of jobs that are printed on Fiery but not yet recorded in PaperCut
        (All params and return are dicts of jobs in PaperCut format)
        
        TODO: 
            Check that this correctly reflects Fiery jobs with 
             Fiery team.
            Find out if there is a way of querying Fiery to return
             only jobs printed since previous query.
    """
    old_keys = set(old_pc_jobs.keys()) 
    new_jobs = {}
    for id,job in pc_jobs.items():
        if id not in old_keys:
            new_jobs[id] = job
        else:
            old_job = old_pc_jobs[id]
            if job['total-pages'] > old_job['total-pages']:
                new_job = copy.copy(job)
                new_job['total-pages'] = job['total-pages'] - old_job['total-pages']
                new_job['total-color-pages'] = job['total-color-pages'] - old_job['total-color-pages']
                new_jobs[id] = new_job
    return new_jobs            

#
# Command line processing
#    
DEFAULT_FIERY_API_KEY_FILE = 'fiery.api.key'   
DEFAULT_FIERY_IP = '192.68.228.104'   
DEFAULT_FIERY_USER = 'admin' 
DEFAULT_FIERY_PWD = 'Fiery.color' 

DEFAULT_PAPERCUT_IP = 'localhost'
DEFAULT_PAPERCUT_PORT = 9191
DEFAULT_PAPERCUT_PWD = 'password'
DEFAULT_PAPERCUT_ACCOUNT = 'Fiery-account'

DEFAULT_SLEEP_SECS = 60

parser = optparse.OptionParser('python %s [options]' % sys.argv[0])
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
print(options.fiery_api_key_file)
print('Fiery API Key="%s"' % fy_api_key) 
fy_url,fy_session_cookie = fiery_login(fy_api_key, options.fiery_ip, 
        options.fiery_user, options.fiery_pwd, options.verbose) 

# Initialize PaperCut        
pc_auth_token = options.papercut_pwd
pc_account = options.papercut_account
pc_server = papercut_init(options.papercut_ip, options.papercut_port,
        pc_auth_token, options.papercut_account)   

# Load record of Fiery jobs already logged on PaperCut
# TODO: Store this in PaperCut in some way        
old_pc_jobs = load_object('fiery.papercut.old.jobs', {})         

# Main loop
#   Poll Fiery for list of printed jobs
#   Convert jobs to PaperCut format
#   Compare to record of jobs already logged in PaperCut
#   If there are any new jobs   
#       Log new jobs
#       Update list record of jobs already logged
while True:  

    print('-' * 80)  
    # Fetch Fiery jobs
    fy_jobs = fiery_fetch_jobs(fy_url, fy_session_cookie, options.verbose)
    print('Fetched %d jobs from %s' % (len(fy_jobs), fy_url))
    if options.verbose:
        print(fy_print_jobs) 

    # Convert Fiery jobs to PaperCut format
    pc_jobs = dict((job['id'],job_fiery_to_papercut(job, pc_account)) for job in fy_jobs)
    
    # Find out which of the Fiery jobs are new
    new_pc_jobs = get_new_jobs(old_pc_jobs, pc_jobs)
    if new_pc_jobs:
        # Log new Fiery jobs on PaperCut 
        print('%d new jobs' % len(new_pc_jobs))
        if options.verbose:
            print(new_pc_jobs) 
        papercut_log_jobs(pc_server, pc_auth_token, pc_jobs)    
        
        # Update record of jobs already logged
        old_pc_jobs = pc_jobs
        save_object('fiery.papercut.old.jobs', old_pc_jobs)   

    if options.verbose:
        print('Sleeping %d sec' % options.sleep_secs)
    time.sleep(options.sleep_secs)
