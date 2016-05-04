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
class FakedMintedEngine(object):
    gamestates = [{"requestaction": "freespin", "winnings": 12, "stake": 15, "win":20},
                {"requestaction": "freespin", "winnings": 12, "stake": 15, "win":20},
                {"requestaction": "spin", "winnings": 12, "stake": 15, "win":20}]
    
    def play(self):
        gamestate = FakedMintedEngine.gamestates.pop()
        if gamestate != None:
            return gamestate;
    
class JobConfig(object):
    kv_cfg = []
    pass

class Job(object):
    def __init__(self):
        self.te = FakedMintedEngine();
        
        self.raw_stats = None
        
        # this will be eventually transmitted to reducer as pickle on network
        self.mapped_stats = {"base_game_spins":0,
                              "base_game_total_stake":0,
                              "free_game_spins":0,
                              "free_game_total_stake":0,
                              "total_spins": 0,
                              "total_stake": 0,
                              "total_wins": 0,
                              "max_stake": []}
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
        jcfg = JobConfig();
        jcfg.kv_cfg.append({"key": "spins", "value": "gamestate"});
        jcfg.kv_cfg.append({"key": "base_game_spins", "value": "number"});
        jcfg.kv_cfg.append({"key":  "base_game_total_stake", "value":"number"});
    
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
            
    def map_stats(self):
        '''
        @aim
        this mtd maps stats based on transaction logic, 
        you may add new kv pair or alert existing kv pairs
        @note
        put any kv that can NOT be combinated together based on transaction logic
        eg, caculate the max stake a palyer pays 
        '''
        self.mapped_stats["max_stake"].append(self.raw_stats["stake"]);
        
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
    
class MyJob(Job):
    def __init__(self):
        Job.__init__(self)
        
    def spin(self):
        pass;

if __name__ == '__main__':
     pass
