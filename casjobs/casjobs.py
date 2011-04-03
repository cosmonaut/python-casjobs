"""
The CASJobs python interface.

Author: Nico Nell (nicholas.nell@colorado.edu)
"""


from ZSI.ServiceProxy import ServiceProxy

# only for debugging
import sys

class CASJobs(ServiceProxy):
    """
    The casjobs client.

    Required arguments:

        *wsid*: [ int ]
        The user's unique casjobs numerical id (This number is found
        on the casjobs profile page)

        *pw*: [ string ]
        The user's casjobs password

    Optional Arguments:

        *url*: [ string ]
        A url pointing to the casjobs WSDL file (Default
        http://cas.sdss.org/CasJobs/services/jobs.asmx?WSDL)

    """

    def __init__(self,
                 wsid,
                 pw,
                 url = "http://cas.sdss.org/CasJobs/services/jobs.asmx?WSDL"):

        #ServiceProxy.__init__(self, url, force = True)
        # Use this line with (import sys) for debugging
        ServiceProxy.__init__(self, url, tracefile=sys.stdout, force = True)

        # casjobs wsid and password for all methods
        self._wsid = wsid
        self._pw = pw

        # We can populate job_types here and have them ready for submit_extract_job

    def get_jobs(self, jobid = None, status = None, includesys = False):
        #test
        """
        Get a list of jobs matching the search parameters.

        
        """

        

        jobres = self.GetJobs(owner_wsid = self._wsid,
                              owner_pw = self._pw,
                              conditions = "taskname : lol;",
                              includeSystem = includesys)

        # Make jobres into job type

        # Return array of job objects

        return jobres

    def get_job_status(self, jobid):
        """
        Get the current status of a job

        Required Arguments:

            *jobid*: [int] 
            The ID number of the job to cancel.

        Returns:

            A string representing the job status:

            - ready
            - started
            - canceling
            - cancelled
            - failed
            - finished

        """

        jobstatus = {0 : "ready",
                     1 : "started",
                     2 : "canceling",
                     3 : "cancelled",
                     4 : "failed",
                     5 : "finished"}

        # Note inconsistency in WSDL camelcase...
        status_key = self.GetJobStatus(wsId = self._wsid,
                                       pw = self._pw,
                                       jobId = jobid)

        
        return jobstatus[status_key['GetJobStatusResult']]

    def get_job_types(self):
        # Is this useful?
        # If so wrap output in a dict or object...
        types = self.GetJobTypes(wsid = self._wsid, pw = self._pw)
        return types

    def cancel_job(self, jobid):
        """
        Cancel a job.

        Required Arguments:

            *jobid*: [int]
            The ID number of the job to cancel.

        Returns:

            Nothing.
        """

        self.CancelJob(wsId = self._wsid, pw = self._pw, jobId = jobid)
        
        return

    def quick_job(self, qry = "", db = "MyDB", taskname = "pyquickjob", savefile = None, issystem = False):
        """
        Execute a quick job

        Required Arguments:

            *qry*: [ string ]
            A SQL query to run.

        Optional Arguments:

            *db*: [ string ]
            The name of the database to query (Default is "MyDB")

            *taskname*: [ string]
            A name identifying the job (Default is "pyquickjob").

            *savefile*: [ string ]
            Name of a file to save CSV output to. (Default is None).

            *issystem*: [ bool ]
            Show or hide system jobs (Default is False).
            

        Returns:

            [ list of strings ]
            A list of strings holding results of the query.
        
        """

        #qry = "SELECT top 10 objid, flags FROM PhotoTag"

        quickres = self.ExecuteQuickJob(wsid = self._wsid,
                                        pw = self._pw,
                                        qry = qry,
                                        context = db,
                                        taskname = taskname,
                                        isSystem = isSystem)

        # Make the giant string into a list of results
        qryres = quickres['ExecuteQuickJobResult'].split("\n")

        if savefile:
            f = open(savefile + ".csv", 'w')
            try:
                for line in qryres:
                    f.write("%s\n" % line)
                    
            except:
                f.close()
                print("WARNING: save file failed to write!")
        
        return qryres


    def submit_extract_job(self, tablename, output_type):
        #test this function
        """
        Submit a table extraction job.
        
        """

        jobid = self.SubmitExtractJob(wsid = self._wsid,
                                      pw = self._pw,
                                      tableName = tablename,
                                      type = output_type)
        
        
        return jobid

    def submit_job(self, qry = "", db = "MyDB", taskname = "pyjob", estimate = 1):
        #test me
        """
        Submit a query job.

        Required arguments:

            *qry*: [ string ]
            A SQL query string

        Optional arguments:

            *db*: [ string ]
            The database to query (Default is MyDB).

            *taskname*: [ string ]
            A name identifying this job (Default is "pyjob").

            *estimate*: [ int ]
            An estimate of the job time in *minutes* (Default is 1).

            Note that the query will be executed in the context mode
            nearest to the estimate given. The context modes are
            generally spaced in intervals of 500 minutes. These
            context modes can be viewed using get_queues().
        """

        jobid = self.SubmitJob(wsid = self._wsid,
                               pw = seld._pw,
                               qry = qry,
                               context = db,
                               taskname = taskname,
                               estimate = estimate)

                
        return jobid


    def get_queues(self):
        """
        Get the queues.

        Required Arguments:

            None

        Returns:

            A list of dicts containing the queue db and timeout
        
        """

        qs = self.GetQueues(wsid = self._wsid,
                            pw = self._pw)

        qlist = []
        for q in qs['GetQueuesResult']['CJQueue']:
            qlist.append({'db' : q['Context'], 'timeout' :q['Timeout']})
        
        return qlist

    def get_output(self):
        # Download from an output url returned by get_jobs
        return


    def upload_data(self, tablename, data, exists = False, ):
        #test
        """
        Upload ASCII encoded CSV data into a table in MyDB.

        Required arguments:

            *tablename*: [ string ]
            The name of the table into which data will be loaded

            *data*: [ file object or file name? (file object or string) ]
            File containing the ASCII encoded CSV data to upload

        Optional arguments:

            *exists*: [ bool ]:
            If True, expects 'tablename' to exists and tries to load
            data into said table using the schema to determine types.
            If False, creates a new table and tries to guess an
            appropriate schema. (Default is False)

        Returns:

            Nothing.
        """

        if type(data) == str:
            #open file
            pass

        # load file into datastr

        self.UploadData(wsid = self._wsid,
                        pw = self._pw,
                        tableName = tablename,
                        data = datastr,
                        tableExists = exists)
        
        return
