from root import Job
from root import JobConfig

class FakedJobSubmitter(object):
    def __init__(self):
        job = Job();
        self.job_config = job.get_job_config()
        
    def submit_job(self):
        # send request to initor for init a new job id
        self.job_config.configs['']
        pass
    
class FakedInitor(object):
    def __int__(self):
        del Job

class FakedMapper(object):
    pass

class FakedReducer(object):
    pass