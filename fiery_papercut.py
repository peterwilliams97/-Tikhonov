# -*- coding: utf-8 -*-
"""
    Script to track Fiery job in PaperCut.

    Assumes access to a Fiery and a PaperCut server.
    
    Uses Fiery cost accounting API

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
        Enable page level color detection for Fiery printers

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
import logging
import csv

#
# Utility functions
#

PRETTY_PRINTER =  pprint.PrettyPrinter(indent=4)

def pprint(obj):
    """Pretty print object obj"""
    PRETTY_PRINTER.pprint(obj)


def log_error(s):
    logging.error(s)
    print >> sys.stderr, s


def log_info(s):
    logging.info(s)
    pprint(s)


def log_debug(s):
    logging.debug(s)


def first_non_empty(iterable):
    """Return first non-empty element in iterable"""
    return next(s for s in iterable if s)   


def get_uid():
    """Return an OS generated 21 character ASCII unique string"""
    return os.urandom(16).encode("base64")[:21]    
    
#
# Fiery code
# 
class FieryState:
    """Stores a Fiery's login info and the state of the number of jobs that have been read from it
        and recorded on a PaperCut server.
            ip: Fierye network name or ip addres
            username: Username for Fiery login
            password: Password for Fiery login
            max_id: Highest job id from this Fiery recorded on PaperCut
            pending_max_id: Highest job id from this Fiery about to be recorded on PaperCut
    """

    def __init__(self, ip=None, username=None, password=None, max_id=None, pending_max_id=None):
        """All params optional to simplify construction from a dict as in from_dict()
        """
        self.ip = ip
        self.username = username
        self.password = password
        self.max_id = max_id 
        self.pending_max_id = pending_max_id

    def __repr__(self): 
        return repr(dict((k,v) for k,v in self.__dict__.items() if v is not None)) 

    def repr_no_ip(self):
        """Convenience function for when ip is stored separately as in PaperCut config editor
            where the ip is stored in the key and repr_no_ip() is stored in the value
        """
        dct = (dict((k,v) for k,v in self.__dict__.items() if v is not None))
        if 'ip' in dct:
            del dct['ip']
        return repr(dct)    

    @classmethod 
    def from_dict(cls, dct, ip=None):
        fiery = cls(**dct)
        if ip:
            fiery.ip = ip
        return fiery

    def is_inconsistent(self):
        """A FieryState is consistent if there are no pending jobs that have not been recorded
            A FieryState can be inconsistent if this script crashed while recoring Fiery jobs on
            a PaperCut server.
            If this ever happens then the PaperCut server should be checked which Fiery jobs with
            with ids with max_id > id >= pending_maxid have been recorded then the FieryState
            should be updated
            The FieryState will be saved in the PaperCut config key given by PaperCut.config_key()
            which will be logged at ERROR level if this should ever happen. See log_inconsistent(). 
        """
        return self.pending_max_id is not None and (
                self.max_id is None or self.pending_max_id > self.max_id)


def fiery_load_api_key(key_file): 
    """Load api key from key_file"""
    try:   
        return open(key_file, 'rb').read()
    except Exception, e:
        print('Could not load Fiery API key from %s: %s' % (key_file, e))
        return None


class FieryConnection:

    count = 0

    @staticmethod
    def set_api_key(api_key):
        FieryConnection.api_key = api_key
        
    def __init__(self, fiery):
        self.fiery = fiery
        self.connected = False
        self.failure = None
        self.login()

    def login(self): 
        """Login to Fiery
            Set connected = True on success
        """

        auth = {
            'username': self.fiery.username, 
            'password': self.fiery.password,
            'accessrights': {'a1': FieryConnection.api_key}
        }
        log_debug('fiery_login: fiery=%s,auth=%s' % (self, json.dumps(auth)))

        if not self.fiery.ip:
            self.failure = 'no server specified'
            return

        self.url = 'https://%s/live' % self.fiery.ip
        
        r = requests.post('%s/login' % self.url, 
                          data=json.dumps(auth), 
                          headers={'content-type': 'application/json'}, 
                          verify=False)
        if r.status_code != 200:
            self.failure = 'login: http code=%s' + str(r.status_code)
            return

        log_debug('Connected to Fiery "%s"' % self.url)

        # Check for cookies
        if 'set-cookie' not in r.headers:
            self.failure = 'No cookies'
            return

        # Extract login coookie
        cookies = r.headers['set-cookie']
        m = re.search('_session_id=([^ ;]+)', cookies)
        if not m:
            self.failure = 'No session cookie'
            return

        self.session_cookie = m.group(1)
        log_debug('session_cookie=%s' % self.session_cookie)

        self.connected = True

    def fetch_jobs(self): 
    
        start_id = self.fiery.max_id + 1 if self.fiery.max_id is not None else 0

        # Request job log
        headers = {'Cookie': '_session_id=%s;' % self.session_cookie}
        full_url = '%s/api/v1/cost?start_id=%d&count=%d' % (self.url, start_id, FieryConnection.count)

        log_debug('Retrieving Fiery jobs: url="%s"' % full_url)

        r = requests.get(full_url, headers=headers, verify=False)
        if r.status_code != 200:
            self.failure = 'url=%s, http code=%d' % (full_url, r.status_code)
            return None

        # The list of printed jobs
        return json.loads(r.text)

        
#
# Job conversion/manipulation code
#
def convert_boolean(fy_bool_str):
    """Convert a Fiery boolean string to a PaperCut boolean string
        Fiery is Yes/No
        PaperCut is TRUE/FALSE
    """
    first = fy_bool_str[0].lower() if fy_bool_str else None
    return 'TRUE' if first == 'y' else 'FALSE'

    
def convert_time(fiery_time):
    """Convert a Fiery date-time to a PaperCut date-time

        fiery_time: Fiery date-time local time localized into Fiery's locale
            We assume all the Fierys that PaperCut is connnected to have the 
            same locale as the computer this script is running on
        Returns: PaperCut date-time corresponding to fiery_time if fiery_time 
            is parsed successfully or an obviously bogus date-time if
            it is not.
    """ 
    try:
        tm = time.strptime(fiery_time, '%H:%M %b %d, %Y')  
        dt = datetime.datetime(*tm[:6])
        return dt.isoformat().replace(':', '').replace('-', '')
    except e:
        print('convert_time: Invalid fiery_time="%s": e' % (fiery_time, e))
        return '11111111T111111'


# TODO: Replace job[key] with job.get(key,None) to make FIERY_PAPERCUT_MAP
#      resilient against missing Fiery keys

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

def convert_job(fiery_job):
    """Convert a Fiery job to a PaperCut job
        TODO: Check this conversion with Fiery team
    """    
    pc_job = dict((k, FIERY_PAPERCUT_MAP[k](fiery_job)) for k in FIERY_PAPERCUT_MAP) 
    pc_job['grayscale'] = 'TRUE' if pc_job['total-color-pages'] > 0 else 'FALSE'
    return pc_job 


class PaperCut:

    def __init__(self, host_name='localhost', port=9191, auth_token=None, account_name=None):
        """ 
            host_name: Network name/IP address of PaperCut server
            port: PaperCut server port to use
            auth_token: Authorization token or password
            account_name: Name of PaperCut Shared Account to be used to store Fiery jobs
        """
        self.host_name = host_name
        self.port = port
        self.auth_token = auth_token
        self.account_name = account_name
        self.connected = False
        self.uid = get_uid()
        self.connect()

    def connect(self):
        """Standard PaperCut XMLRPC initialization
            Instantiate a server and create the accout if it doesn't 
            already exist.
        """

        log_info('Connecting to PaperCut "%s:%d"' % (self.host_name, self.port))

        self.server = xmlrpclib.Server('http://%s:%d/rpc/api/xmlrpc' % (self.host_name, self.port))

        self.claim()

        if self.server.api.isSharedAccountExists(self.auth_token, self.account_name):
            log_info('PaperCut Shared Account "%s" account already exists.' % self.account_name)
        else:
            # user does not exist so create
            log_info('Creating PaperCut Shared Account "%s"' % self.account_name)
            self.server.api.addNewSharedAccount(self.auth_token, self.account_name)

        self.connected = True     

    def convert_job(self, fiery_job):
        """Convert a Fiery job to a PaperCut job and apply a server shared-accout name
        """    
        pc_job = convert_job(fiery_job)
        pc_job['server'] = self.host_name
        pc_job['shared-account'] = self.account_name
        return pc_job

    @staticmethod
    def validate_jobs(fiery, fiery_job_list):
        """Validate new job ids against those already stored on PaperCut
        """
        if fiery.is_inconsistent():
            log_inconsistent(fiery) 
            return False 

        for job in fiery_job_list: 
            if fiery.max_id is not None and job['id'] > fiery.max_id:
                log_error('\n\t'.join([
                            'Fetched Fiery job id <= previous max id',
                            'job[id]=%s,fiery.max_id=%s', 
                            'job=%s',
                            'previous state=%s']) % (
                        job['id'], fiery.max_id, job, fiery))
                return False
 
        return True    

    def _record_jobs_int(self, fiery_job_list):
        """Record Fiery jobs in PaperCut job log

            server: PaperCut XMLRPC server instance
            auth_token: Authorization token or password
            pc_jobs: dict of jobs to record in PaperCut job log (in PaperCut format)
        """
        for fiery_job in fiery_job_list:
            job = self.convert_job(fiery_job)
            # PaperCut doesn't log non-printing print jobs
            if job['total-pages'] <= 0:
                continue
            # See http://www.papercut.com/products/ng/manual/ "Importing Print Job Details"    
            job_details = ','.join('%s=%s' % (k,v) for k,v in job.items())
            print('Recording job="%s"' % job_details)
            self.server.api.processJob(self.auth_token, job_details)    

    def record_jobs(self, fiery, fiery_job_list):

        if not PaperCut.validate_jobs(fiery, fiery_job_list):
            return False

        self.check_claim()

        max_id = max(job['id'] for job in fiery_jobs)
        fiery.pending_max_id = max_id  
        self.save_fiery(fiery)

        self._record_jobs_int(fiery_job_list)

        fiery.max_id = max_id
        fiery.pending_max_id = None
        self.save_fiery(fiery)

        return True
 
    FIERY = 'Fiery'
    FIERY_LIST = '%s.list' % FIERY 
    FIERY_CLAIM = '%s.claim' % FIERY    

    @staticmethod
    def config_key(fiery_ip):
        """PaperCut config key format used for storing Fiery state"""
        return '%s:%s' % (PaperCut.FIERY, fiery_ip)

    def claim(self):
        """Assert the claim current instance of this script to update PaperCut with Fiery jobs."""
        self.server.api.setConfigValue(self.auth_token, PaperCut.FIERY_CLAIM, self.uid) 

    def check_claim(self):
        """Check if another instance of this program is updating PaperCut.
            Exit if it is, after telling it to exit as well
        """
        uid = self.server.api.getConfigValue(self.auth_token, PaperCut.FIERY_CLAIM)
        if uid != self.uid:
            log_error('Another instance of this program is updating PaperCut server "%s" 
                        % self.host_name)
            # Tell the other instance to shutdown
            self.claim()                
            exit(11)

    def save_fiery(self, fiery):  
        """Save Fiery state in PaperCut config."""
        assert fiery.__class__.__name__ == 'FieryState', fiery.__class__.__name__
        fiery_ip = fiery.ip
        fiery_str = fiery.repr_no_ip()
        self.server.api.setConfigValue(self.auth_token, PaperCut.config_key(fiery_ip), fiery_str)  

    def load_fiery(self, fiery_ip):  
        """Load Fiery state from PaperCut config
            fiery_ip: IP adddress/netword name of Fiery for which start id is being saved
            Returns: Fiery state
            Always returns a dict with as much info as it can get from the PaperCut config.
        """
        fiery_str = self.server.api.getConfigValue(self.auth_token, PaperCut.config_key(fiery_ip))
        fiery_dct = eval(fiery_str) if fiery_str else {}
        return FieryState.from_dict(fiery_dct, ip=fiery_ip)

    def save_fiery_ip_list(self, fiery_ip_list):
        fiery_str = repr(fiery_ip_list)
        self.server.api.setConfigValue(self.auth_token, PaperCut.FIERY_LIST, fiery_str) 

    def load_fiery_ip_list(self):
        fiery_str = self.server.api.getConfigValue(self.auth_token, PaperCut.FIERY_LIST)
        if not fiery_str:
            return []
        return eval(fiery_str)

    def load_fiery_list(self):
        self.check_claim()
        fiery_ip_list = self.load_fiery_ip_list()
        return [self.load_fiery(fiery_ip) for fiery_ip in fiery_ip_list]

    def save_fiery_list(self, fiery_list):
        self.check_claim()
        fiery_ip_list = [fiery.ip for fiery in fiery_list]
        self.save_fiery_ip_list(fiery_ip_list)
        for fiery in fiery_list:
            self.save_fiery(fiery)
            
    def current_status()        
  

def log_inconsistent(fiery):
    log_error('Inconsistent Fiery state=%s in PaperCut config key=%s' 
                % (fiery, PaperCut.config_key(fiery.ip))

                
def check_consistency(fiery_list):   
    """Check that all Fiery states are consistent and exit if they are not"""   
    num_inconsistent = 0   
    for fiery in fiery_list:
        if fiery.is_inconsistent():
            log_inconsistent(fiery)
            num_inconsistent += 1
    if num_inconsistent > 0:
        exit(2)

 
def describe_papercut(papercut):
    msg = \
'''The Fiery jobs tracked so far can be seen in the PaperCut web admin interface
    
''' FIERY = 'Fiery'
    PFIERY_LIST = '%s.list' % FIERY 
    FIERY_CLAIM = '%s.claim' % FIERY    

 
def load_fierys_csv(csv_path):
    """Load a list of Fiery ip, username, passwords from a CSV file"""
    fiery_list = []
    try: 
        with open(csv_path, 'rb') as csv_file:
            reader = csv.reader(csv_file)
            for i,row in enumerate(reader):
                if len(row) != 3:
                    log_error('load_fierys_csv: row %d is formatted incorrectly row=%s' % (i, row))
                    fiery_list = None
                    break
                ip,username,password = row    
                fiery_list.append(FieryState(ip=ip, username=username, password=password))

    except IOError, e:
        log_error('dump_fierys_csv: csv_path="%s" failed with %s' % (csv_path, e)) 
        return None
    return fiery_list  
    

def dump_fierys_csv(csv_path, fiery_list):
    """Save a list of Fiery ip, username, passwords to a CSV file"""
    try: 
        with open(csv_path, 'wb') as csv_file:
            writer = csv.writer(csv_file)
            for fiery in fiery_list:
                writer.writerow([fiery.ip, fiery.username, fiery.password])
    except IOError, e:
        log_error('dump_fierys_csv: csv_path="%s" failed with %s' % (csv_path, e)) 
        return False
    return True 
    

def process_command_line():    
    """Process the command line.
        This script can only reasonably be run on the PaperCut server it is communicating with
        so it does not make sense to change
            - papercut_ip

    """   
    DEFAULT_FIERY_API_KEY_FILE = 'fiery.api.key'   
    DEFAULT_FIERY_IP = None   # '192.68.228.104'   
    DEFAULT_FIERY_USER = None # 'admin' 
    DEFAULT_FIERY_PWD = None  # 'Fiery.color' 
    DEFAULT_FIERY_BATCH_SIZE = 1000 

    DEFAULT_PAPERCUT_IP = 'localhost'
    DEFAULT_PAPERCUT_PORT = 9191
    DEFAULT_PAPERCUT_PWD = 'password'
    DEFAULT_PAPERCUT_ACCOUNT = 'Fiery-account'

    DEFAULT_SLEEP_SECS = 60

    parser = optparse.OptionParser('python %s [options]' % sys.argv[0])
    parser.add_option('-L', '--csv-load', dest='csv_load',  
            default=None, 
            help='Load Fiery ip, username, pwd from csv file')
    parser.add_option('-D', '--csv-dump', dest='csv_dump',  
            default=None, 
            help='Dump Fiery ip, username, pwd to csv file')            
    parser.add_option('-B', '--fiery-batch-size', dest='fiery_batch_size', 
            default=DEFAULT_FIERY_BATCH_SIZE, 
            help='Number of jobs to request in each call to Fiery')
    parser.add_option('-K', '--fiery-api-key', dest='fiery_api_key_file', 
            default=DEFAULT_FIERY_API_KEY_FILE, 
            help='Path of Fiery API key file')        
    parser.add_option('-S', '--fiery-ip', dest='fiery_ip', 
            default=DEFAULT_FIERY_IP, 
            help='Network name or IP address of Fiery to monitor')
    parser.add_option('-U', '--fiery-user', dest='fiery_user', 
            default=DEFAULT_FIERY_USER, 
            help='Fiery username')
    parser.add_option('-P', '--fiery-pwd', dest='fiery_pwd', 
            default=DEFAULT_FIERY_PWD, 
            help='Fiery password')
    parser.add_option('-s', '--papercut-ip', dest='papercut_ip', 
            default=DEFAULT_PAPERCUT_IP,
            help='Network name or IP address of PaperCut server') 
    parser.add_option('-o', '--papercut-port', dest='papercut_port', type='int', 
            default=DEFAULT_PAPERCUT_PORT, 
            help='PaperCut port')
    parser.add_option('-p', '--papercut-pwd', dest='papercut_pwd', 
            default=DEFAULT_PAPERCUT_PWD, 
            help='PaperCut admin password')
    parser.add_option('-a', '--papercut-account', dest='papercut_account', 
            default=DEFAULT_PAPERCUT_ACCOUNT, 
            help='Name of PaperCut shared account to log Fiery prints in')
    parser.add_option('-v', '--verbose', action='store_true', dest='verbose', 
            default=False, 
            help='verbose output')     
    parser.add_option('-t', '--sleep-secs', dest='sleep_secs', type='int', 
            default=DEFAULT_SLEEP_SECS, 
            help='Sleep time between successive Fiery polls')    
    parser.add_option('-i', '--ignore-history', action='store_true', dest='ignore_history', 
            default=False, 
            help='Ignore history of Fiery jobs logged on PaperCut and log all jobs again.')
    parser.add_option('-d', '--debug', action='store_true', dest='debug', 
            default=False, 
            help='Enable debug logging')        

    options,args = parser.parse_args()

    # 
    # We can't check command line params as they are optional (i.e. args is empty) 
    # so we just print them to stdout.
    #  
    print('-' * 80)
    print(__doc__)
    print('     --help for more information')
    print('-' * 80)
    log_info('Options')    
    log_info(options.__dict__)
    log_info('-' * 80)

    return options,args

   
def main():
    """Top level processing
    """
    
    logging.basicConfig(
        filename='papercut.fiery.log',
        format='%(asctime)s %(levelname)s: %(message)s',
        level=logging.INFO)
        
    logging.info(' Starting '.join(['=' * 30] * 2))
    
    options,args = process_command_line()

    if options.debug:
        logging.basicConfig(level=logging.DEBUG)

    # Initialize PaperCut        
    papercut = PaperCut(options.papercut_ip, options.papercut_port, options.papercut_pwd, 
                        options.papercut_account)  
    if not papercut.connected:
        log_error('Could not connect to PaperCut: papercut=%s' % papercut) 
        exit(1)

    # Fetch the list of Fiery states stored on PaperCut  
    fiery_list = papercut.load_fiery_list()  

    # We will check consistency throughout this script
    check_consistency(fiery_list)  

    if options.csv_load:
        # Load Fiery IPs, usernames and passwords from csv file
        fiery_list = load_fierys_csv(options.csv_load)
        if not fiery_list:
            log_error('Could not load Fierys from csv file="%s"' % options.csv_load)
            exit(3)    

        # Update with fiery_list any state stored for these Fierys on PaperCut
        for fiery in fiery_list:
            pc_fiery = papercut.load_fiery(fiery.ip)
            if pc_fiery.is_inconsistent():
                log_inconsistent(pc_fiery) 
                exit(4)
            fiery.max_id = pc_fiery.max_id

        # Save Fierys to PaperCut config       
        papercut.save_fiery_list(fiery_list)

    if options.fiery_ip or options.fiery_user or options.fiery_pwd:
        if not (options.fiery_ip and options.fiery_user and options.fiery_pwd):
            log_error('If any of fiery_ip, fiery_user or fiery_pwd is specified then all must be')
            exit(5)
        if options.csv_load:
            log_error('If any of fiery_ip, fiery_user or fiery_pwd is specified then csv_load may not be')
            exit(6)   

        fiery = FieryState(options.fiery_ip, options.fiery_user, options.fiery_pwd)
        pc_fiery = papercut.load_fiery(fiery.ip)
        if pc_fiery.is_inconsistent():
            log_inconsistent(pc_fiery)  
            exit(7)
        fiery.max_id = pc_fiery.max_id

        fiery_list = [fiery]    
        
    # We will check consistency throughout this script
    check_consistency(fiery_list)     

    if options.csv_dump:
        if not dump_fierys_csv(options.csv_dump, fiery_list):
            exit(8)

    # Clean up the Fierys   
    for fiery in fiery_list:
        fiery.pending_max_id = None
        assert fiery.ip, 'Invalid fiery=%s' % fiery    

    # Check that we have at least one Fiery

    log_info('fiery_list=%s' % fiery_list)  
    if not fiery_list:
       log_error('No Fierys specified. Nothing to do')
       exit(9)        

    # We now have a valid Fiery list
    # So we save it to the PaperCut config
    papercut.save_fiery_list(fiery_list) 

    # This is a debug option
    # TODO: Remove
    if options.ignore_history:  
        for fiery in fiery_list:
            fiery.max_id = None   

    #
    # Initialize connections to all Fierys in  fiery_list
    # 
    api_key = fiery_load_api_key(options.fiery_api_key_file)
    if not api_key:
        exit()
        
    FieryConnection.set_api_key(api_key)    
    FieryConnection.count = options.fiery_batch_size

    log_debug('Fiery API Key file="%s"' % options.fiery_api_key_file)   
    log_debug('Fiery API Key="%s"' % api_key) 
       
    attempted_connection_list = [FieryConnection(f) for f in fiery_list]
    fiery_connection_list = [f for f in attempted_connection_list if f.connected]
    failed_connection_list = [f for f in attempted_connection_list if not f.connected]

    log_info('Attempted to login to %d Fierys. %d succeeded, %d failed' % (
        len(attempted_connection_list), 
        len(fiery_connection_list),
        len(failed_connection_list)
    )) 
    if failed_connection_list:
        log_info('Failures=%s' % failed_connection_list)   
    log_debug('=' * 80)  


    #
    # We now have connections and valid Fiery states in PaperCut so we are ready to go
    #

    #
    # Main loop
    #   Poll alll Fierys for lists of jobs printed since the last time we polled
    #   If there are any new jobs   
    #       record new job in PaperCut
    #
    while True:  

        for fiery_connection in fiery_connection_list:

            fiery_jobs = fiery_connection.fetch_jobs()

            if fiery_jobs:
                log_info('Fetched %d jobs from %s' % (len(fiery_jobs), fiery_connection.fiery.ip))
                log_debug(fiery_jobs) 

                if not papercut.record_jobs(fiery_connection.fiery, fiery_jobs):
                    exit(10)

        log_debug('Sleeping %d sec' % options.sleep_secs)
        log_debug('-' * 80)  
        time.sleep(options.sleep_secs)

        
#
# Execution starts here
#    
main()
        