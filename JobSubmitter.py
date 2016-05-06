# coding=utf-8 

from root import Job
from root import JobConfig
from ipykernel.serialize import cPickle
import zlib
import msgids
import struct
import ctypes

faked_global_msg = None;
faked_global_initor = FakedInitor()
faked_global_submitter = FakedJobSubmitter()

class FakedJobSubmitter(object):
    def __init__(self):
        import parser
        parser = argparse.ArgumentParser()
        parser.add_argument("-s", "--submit", type=str, help="job absulute path")  # required
        parser.add_argument("-p", "--port", type=int, help="reducer's port")  # required
        self.args = parser.parse_args()
        import cPickle
        job = Job();
        self.job_config = job.get_job_config()
               
    def submit_job(self):
        print("submitter logging::submit_job() called\n")
        # send job init msg to remote initor process
        faked_global_msg = msgids.build_msg(msgids.c2i_init_job, self.job_config.configs)
        # self.transport.write(faked_global_msg)
        faked_global_initor.recv(faked_global_msg)
        
    def recv(self, msg):
        msg = cPickle.loads( zlib.decompress(msg))
        msgid = msg[0]
        
        if msgid == msgids.i2c_no_resource:
            print("submitter logging::i2c_no_resource\n")
            jobid = msg[1]
            # submit failed, notify user
            print("your job id is %s\n" % jobid)
            print("as cluster is quite busy now, \
            your job have been appended and \
            will be processed when it becomes avaiable\n")

    
class FakedInitor(object):
    def __int__(self):
        import uuid
        self.free_workers = []
        self.appendingjobs = []
        
    '''
    @fixme add timer within twisted
    '''
    def append_job(self, jobtuple):
        print("append_job()::add timer for this appended job\n")
        pass 
     
    def recv(self, msg):
        msg = cPickle.loads( zlib.decompress(msg))
        msgid = msg[0]
        
        if msgid == msgids.c2i_init_job: # request for a new job from client
            # get job config
            jobconfig = msg[1]
            # allocate uuid fr this job as global indentity
            jobid = uuid.uuid1()
        
            if len(self.free_workers) <= 0: # no even one free mapper or reducers
                print("initor logging::c2i_init_job::no even one free mapper or reducers\n")
                # we will append this job until there are free worker in the furture (jobid->jobconfig)
                # allocate a timer for this appended job
                self.append_job((jobid,jobconfig))
                # tell client no resource to allocate for it
                faked_global_msg = msgids.build_msg(msgids.i2c_no_resource, jobid)
                # self.transport.write(faked_global_msg)
                faked_global_submitter.recv(faked_global_msg)
                return
            
            #### start to allocate job to avaible workers based on user's requeset ###
            print("initor logging::c2i_init_job:: start to allocate job\n")
            job_expected_mapper = jobconfig["job_expected_mapper"]
            job_expected_reducer = jobconfig["job_expected_reducer"]
            
        
    
class FakedMapper(object):
    pass

class FakedReducer(object):
    pass

if __name__ == '__main__':
     FakedJobSubmitter
     pass