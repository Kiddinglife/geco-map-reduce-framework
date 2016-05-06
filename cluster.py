# coding=utf-8 

from root import Job
from root import JobConfig
import msgids

import struct
import ctypes
import cPickle
import argparse
import sys
import os
import uuid

def debug_print(fmt, msg):
    print(fmt % msg)

class FakedJobSubmitter(object):
    '''
3) API user runs it in cmd console to submit a job to map-reduce cluster
  3.2) python  init path2jobmodule/job_module_name:job_class_name -> init a new job to cluster   
         eg. vt init ./engines/minted/root:MintedJob engines dir -> minted dir -> root.py -> MintedJob class
  3.3) vt status : show all jobs' status including job name, running status and so on
  3.4) vt fetch [--show, --store] jobname : fetch the current stats from cluster and
   show it in the stdout or store it in files
    '''
    def __init__(self):
        self.faked_initor = FakedInitor()
        self.faked_initor.faked_submitter = self
        
        import parser
        parser = argparse.ArgumentParser()
        parser.add_argument('-i', '--init', type=str, help="init a new job to map reduce cluster") 
        parser.add_argument('-s', "--status", type=str, help="show all jobs' status including job \
        name, running status and so on")
        parser.add_argument('-f', '--fetch', type=str, help=" vt fetch [--show, --store] jobname \
         fetch the current stats from cluster and show it in the stdout or store it in files") 
        self.args = parser.parse_args()
        jobpath = self.args.init
        status = self.args.status
        fetch = self.args.fetch
        
        
        self.s = msgids.serializer()
        
        if jobpath != None:
            print("init enabled, job path %s" % jobpath)
            
            currdirpath = os.path.abspath(os.path.dirname(__file__))
            # print currdirpath
            strs = jobpath.split(":")
            modulepath = strs[0]
            if modulepath[0] == "." and  modulepath[1] == "/":
                moduleabspath = currdirpath + modulepath[1:]
            elif modulepath[0] != "/" and modulepath[0] != ".":
                moduleabspath = currdirpath + "/" + modulepath
            else:
                moduleabspath = currdirpath + modulepath
            # print moduleabspath
            sys.path.append(moduleabspath)
            (filepath, filename) = os.path.split(modulepath)
            (shortname, extension) = os.path.splitext(filename)
            clientmodule = __import__(shortname)
            classname = strs[1]
            class_type = getattr(clientmodule, classname)
            job = class_type()
            jobconfigs = job.get_job_config()
            self.submit(jobconfigs)
            # print os.path.dirname(filepath)
            # print((filepath, filename, classname))
        elif status != None:
            print("status enabled")
        elif fetch != None:
            print("fetch enabled")
            
#         self.s = msgids.serializer()
#         job = Job();
#         self.job_config = job.get_job_config()
               
    def submit(self, jobconfig):
        print("cluster logging::submit_job() enter")
        # send job init msg to remote initor process
        faked_global_msg = self.s.encode(msgids.c2i_init_job, jobconfig)
        '''
         @fixme remove this is for stub test
        '''
        # self.transport.write(faked_global_msg)
        self.faked_initor.recv(faked_global_msg)
        print("cluster logging::submit_job() leave")
        
    def recv(self, data):
        msgs = self.s.decode(data)
        for msg in msgs:
            msgid = msg[0]
            if msgid == msgids.i2c_no_resource:
                print("submitter logging::i2c_no_resource")
                jobid = msg[1]
                # submit failed, notify user
                print("job id : %s" % jobid)
                print("job status: appended")

    def set_initor_stub(self, faked_initor):
        '''
        @fixme remove this is for stub test
        '''
        self.faked_initor = faked_initor
        
class FakedInitor(object):
    def __init__(self):
        
        '''
        @fixme renove this is for stub
        '''
        self.faked_submitter = None 
        
        self.s = msgids.serializer()
        import uuid
        self.free_workers = []
        self.appendingjobs = []
        
    '''
    @fixme add timer within twisted
    '''
    def append_job(self, jobtuple):
        print("append_job()::add timer for this appended job")
        pass 
     
    def recv(self, data):
         print ("cluster logging::FakedInitor::recv() enter")
         msgs = self.s.decode(data)
         for msg in msgs:
             msgid = msg[0]
             if msgid == msgids.c2i_init_job:  # request for a new job from client
                # get job config
                jobconfig = msg[1]
                # allocate uuid fr this job as global indentity
                jobid = uuid.uuid1()
             
                if len(self.free_workers) <= 0:  # no even one free mapper or reducers
                    print("cluster logging::initor::recv()::c2i_init_job::no even one free mapper or reducers")
                    # we will append this job until there are free worker in the furture (jobid->jobconfig)
                    # allocate a timer for this appended job
                    self.append_job((jobid, jobconfig))
                    # tell client no resource to allocate for it
                    faked_global_msg = self.s.encode(msgids.i2c_no_resource, jobid)
                    '''
                    @fixme remove this is for stub test
                    '''
                    # self.transport.write(faked_global_msg)
                    self.faked_submitter.recv(faked_global_msg)
                else:
                    #### start to allocate job to avaible workers based on user's requeset ###
                    print("initor logging::c2i_init_job:: start to allocate job")
                    job_expected_mapper = jobconfig["job_expected_mapper"]
                    job_expected_reducer = jobconfig["job_expected_reducer"]
                
         print ("cluster logging::FakedInitor::recv() leave")    
    
class FakedMapper(object):
    pass

class FakedReducer(object):
    pass

if __name__ == '__main__':
    faked_submitter = FakedJobSubmitter()
    pass
