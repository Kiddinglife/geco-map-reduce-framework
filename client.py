# coding=utf-8 

'''
Created on 4 May 2016,
Revied on 5 May 2016,
@author: jakez
'''

'''
2) Rewrite GetCofig() method in "JobConfig" class where the user has to specify the items below:
1   set up kv names and types in formate of string:
this is used for reducer to find the value to operate on
example:
("keyid": 0, "keyname": "base_game", "keytype": "string", "valuetype": "container");
("keyid": 1, "keyname": "plays", "keytype":"string", "valuetype":"number");
("keyid": 2, "keyname": "total_stake", "keytype" : "string", "valuetype":"number");
assume reducer receives a intermediate kv pairs [0012] (first 0 is a unique job id allocated by initor)
it will parse these 4 digits to locate the correct place that stores value and valuetype to take the correct operation on it such as sum, find max  and so on. (api user can specify the reduce rules in reduce())
2   set up job name; (this is required when client want to find the previous job they run)
3   set up job order; (this is required when some job are sequenced to run)
4   set up pyc files path for uploading to initor
     4.1
5   set up output and input path in remote machines (this required when backup stats result for re-use)
6   set up if delete the pyc files or keep it for reusage when all done
7   set up output results path in local client (this is required if we want to store the spin resukts in local host)
8   set up if quiet to client (this is required if we want see spin results in local host's console)
9   set up expected mapper count to run (we may get a number less than what we want from initor)
10 set up expected reducer count to run (we may get a number less than what we want from initor)
11 set up heartbeat interval to initor (this is required when some reducer or mapper crashes or get blocked for some reason, we can restart them)
12 set up flag if we are stats sourcer (this is required when the kv pairs need to be generated in real-time, if this flag NOT set-up, this avoid run empty GetStats() method)
13 set up initor ip adress and port if it is located in WAN otherwise leave them alone
(this is required when connect to initor.)
'''
# this means this file must put with the same package with engine file
from engine import FakedMintedEngine

class JobConfig(object):
    def __init__(self):
        self.configs = \
        {
                    # you MUST specify:
                    "job_name": "",
                    "job_unload_paths": None,  # tuple (many abs dir path) unload path to initor
                    # for remote import the module a d insanit job object, 
                    # eg.  assume we run client program in rgscore dir,
                    # you can type python -m cluster --init ./engines/minted/client.py:Job
                    "job_module_load_path":"",
                    "job_connection_address":None,  # tuple (ip, port)
                    
                    # default items
                    "job_num": 1,
                    "job_order": 0,
                    "job_quiet": False,
                    
                    "job_expected_mapper":1,
                    "job_expected_reducer":1,
                    "job_heartbeat_interval":1000,  # ms
        }
        
        # will store the job results
        # False means do not store any stats in local machine drive
        self.configs['job_local_ouput_path'] = "/temp/%s.result.dir" % self.configs["job_name"]
        
        # will be use to store mapped itermediate stats and reduced final stats in text files as backup
        # False means do not store any stats in remote machine drive
        self.configs['job_remote_ouput_path'] = "/temp/%s.output.dir" % self.configs["job_name"]
        
        # will be use to store the job pyc files
        self.configs['job_remote_input_path'] = "/temp/%s.input.dir" % self.configs["job_name"]
        
        
    def job_name(self, name):
        self.configs['job_name'] = name
        self.configs['job_local_ouput_path'] = "/temp/%s.result.dir" % self.configs["job_name"]
        self.configs['job_remote_ouput_path'] = "/temp/%s.output.dir" % self.configs["job_name"]
        self.configs['job_remote_input_path'] = "/temp/%s.input.dir" % self.configs["job_name"]
        
    def job_unload_paths(self, job_unload_paths):
        self.configs['job_unload_paths'] = job_unload_paths
        
    def job_module_load_path(self, job_module_load_path):
        self.configs['job_module_load_path'] = self.configs['job_remote_input_path'] + job_module_load_path
        
        
    def job_num(self, job_num):
        self.configs['job_num'] = job_num
        
    def job_order(self, job_order):
        self.configs['job_order'] = job_order
        
    def job_quiet(self, job_quiet):
        self.configs['job_quiet'] = job_quiet
        
    def job_local_ouput_path(self, job_local_ouput_path):
        self.configs['job_local_ouput_path'] = job_local_ouput_path
        
    def job_expected_mapper(self, job_expected_mapper):
        self.configs['job_expected_mapper'] = job_expected_mapper
        
    def job_expected_reducer(self, job_expected_reducer):
        self.configs['job_expected_reducer'] = job_expected_reducer
        
    def job_heartbeat_interval(self, job_heartbeat_interval):
        self.configs['job_heartbeat_interval'] = job_heartbeat_interval
        
    def job_connection_address(self, job_connection_address):
        
        self.configs['job_connection_address'] = job_connection_address
        
    def job_remote_input_path(self, job_remote_input_path):
        self.configs['job_remote_input_path'] = job_remote_input_path
        
    def job_remote_ouput_path(self, job_remote_ouput_path):
        self.configs['job_remote_ouput_path'] = job_remote_ouput_path

'''   
do not change the the name of Job because the mapper or reducer need to import it
step 
1. add 'job_self_path' into python search path
2. then simply from JobModule import Job
3. then job = Job(), job.get_stats(), job.combina_stats(), job.map_stats().
 '''
class Job(object):
    def __init__(self):
        self.te = FakedMintedEngine();
        
        self.config = JobConfig()
        
        self.raw_stats = None
        # this will be eventually transmitted to reducer as pickle on network
        self.mapped_stats = {"base_game_spins":0,
                              "base_game_total_stake":0,
                              "free_game_spins":0,
                              "free_game_total_stake":0,
                              "total_spins": 0,
                              "total_stake": 0,
                              "total_wins": 0,
                              "max_stake": 0}
        # setup by user
        
        self.reduced_stats = {"base_game_spins":0,
                              "base_game_total_stake":0,
                              "total_spins": 0,
                              "max_stake": 0,
                              "total_wins": 0,
                             "max_stake": 0,
                              "rtp":0}
        
    def get_job_config(self):
        '''
        @aim 
        for serilization for transtion on network between mapper and reducer processes.
        '''
        print("client logging::get_job_config() enter")
        self.config.job_name('test_new_vt')
        root = "/home/jakez/2016209/rgs-core"
        job_unload_paths = (
                    root + "/engines",
                    root + "/OGA",
                    root + "/RNG",
                    root + "/test_engine",
                    )
        self.config.job_unload_paths(job_unload_paths)
        job_module_load_path = "/engines/FakedMintedEngine/client.py:MintedJob"
        self.config.job_module_load_path(job_module_load_path)
        for k, v in self.config.configs.items():
                print((k, v))
        print("client logging::get_job_config() leaving")
        return self.config.configs

        
    def get_stats(self):
        '''
        @aim
        return kv pairs stats
        the stats here refers to raw stats that may come from anywhere in any formate. 
        1.stats prior-existed. eg. text logs.
        2.stats generated in real time. eg. gamestate return from play() method
        @why
        map and reduce framework only accepts stats in the formate of kv pairs.
        so, this mtd can convert the raw data into the aaceptable formate for further precessing
        '''
        self.raw_stats = self.te.play();
         
    def combine_stats(self):
        '''
        @aim 
        to give user an option to aggregate values with the same key
        eg. we can cummulate the value with key of "total_stake". 
        @why
        with combining, the framework can work in a more efficient way 
        as we send less network data to reducer
        @note
        put any kv that can be combinated together based on transaction logic
        eg. total spins
        '''
        self.mapped_stats["total_spins"] += 1
        self.mapped_stats["totak_stake"] += self.raw_stats["stake"];
        self.mapped_stats["total_wins"] += self.raw_stats["win"];
        
        if(self.raw_stats["requestaction"] == "spin"):
            self.mapped_stats["base_game_total_stake"] += self.raw_stats["winnings"];
            self.mapped_stats["base_game_spins"] + 1;
        else:
            self.mapped_stats["free_game_total_stake"] += self.raw_stats["winnings"];
            self.mapped_stats["free_game_spins"] + 1;
            
        if self.mapped_stats["max_stake"] < self.raw_stats["stake"]:
            self.mapped_stats["max_stake"] = self.raw_stats["stake"]
        
    def map_stats(self):
        '''
        @aim
        this mtd maps stats based on transaction logic, 
        you may add new kv pair or alert existing kv pairs
        @note
        put any kv that can NOT be combinated together based on transaction logic
        eg, caculate the max stake a palyer pays 
        '''
        pass
        
    def reduce_stats(self):
        '''
        @aim
        this mtd reduces stats based on transaction logic, 
        you may add new kv pair or alert existing kv pairs
        @note
        if you do NOT need reducer function, just leave it empty and mention it in config files
        '''
        # find max stake from max_stske key we assume it is first one as this is test
        self.reduced_stats["max_stake"] = self.mapped_stats["max_stake"][0]
        # caculate rtp
        self.reduced_stats["rtp"] = self.mapped_stats["total_wins"] / self.mapped_stats["total_stake"];
