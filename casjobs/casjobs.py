"""
The CASJobs python interface.

Author: Nico Nell (nicholas.nell@colorado.edu)
"""


from ZSI.ServiceProxy import ServiceProxy
import sys, os


class CASJobsClient(ServiceProxy):
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

        *debug*: [ bool ]
        Print raw ServiceProxy output to stdout if True (Default is
        False).
    """

    def __init__(self,
                 wsid,
                 pw,
                 url = "http://cas.sdss.org/CasJobs/services/jobs.asmx?WSDL",
                 debug = False):

        if debug:
            ServiceProxy.__init__(self, url, tracefile=sys.stdout, force = True)
        else:
            ServiceProxy.__init__(self, url, force = True)

        # casjobs wsid and password for all methods
        self._wsid = wsid
        self._pw = pw

        self.types = self._get_job_types()

    def _get_job_types(self):
        """
        Get job types

        This information can be accessed in self.types.
        """
        types = self.GetJobTypes(wsid = self._wsid, pw = self._pw)

        types = types['GetJobTypesResult']['CJType']
            
        return types

    def get_jobs(self, jobid = "", timesubmit = "", timestart = "", timeend = "",
                 status = "", queue = "", taskname = "", error = "", query = "",
                 context = "", type = "", wsid = "", includesys = False):
        """
        Get a list of jobs matching the search parameters.

        Optional arguments:

            *jobid*: [ string ]
            Unique identifier of job

            *timesubmit*: [ datetime formatted string ]
            When the job was submitted

            *timestart*: [ datetime formatted string ]
            When the job was started
            
            *timeend*: [ datetime formatted string ]
            When the job completed/cancelled/failed
            
            *status*: [ string ]
            A number representing job status:
            
            0. ready
            1. started
            2. canceling
            3. cancelled
            4. failed
            5. finished

            *queue*: [ string ]
            A number representing the queue.

            *taskname*: [ string ]
            The user submitted descriptor for the job.

            *error*: [ string ]
            Error message for this job, if any

            *query*: [ string ]
            The query submitted for this job

            *context*: [ string ]
            The context of this job (database name)

            *type*: [ string ]
            The type of this job. See get_job_types for a listing of possible values.

            *wsid*: [ string ]
            The WebServicesID of the owner of this job. If owner_wsid
            does not have the 'admin' privilege, then this has no
            effect.

            The parameters are specifically formatted strings
            describing which jobs should be retrieved.  The strings
            are formatted as a '|' separated list of conditions.  Each
            condition may have one of the following possible formats:
            
            - VALUE (equality) 
            - VALUE, (equal or greater to VALUE)
            - ,VALUE (less than or equal to VALUE)
            - V1,V2 (between V1 and V2 (inclusive))
            
            String and DateTime values should not be quoted and may
            not contain any special characters (:;,|).  DateTime
            values can be any format that .net will parse, so long as
            they do not contain any of the above special characters.
            
            The jobs returned are determined by the intersection of
            the keys, given the union of their conditions
            
            Example 1:
            jobid = "12345"
            Jobs with a jobid equal to 12345.
            
            Example 2:
            jobid = "123|321|132"
            Jobs with a jobid equal to 123 or 321 or 132.
            
            Example 3:
            jobid = "123|321", status = "5"
            Jobs with a status of 5 and a jobid of 123 of 321.
            
            Example 4:
            jobid = "123,|,122"
            Returns jobs with id greater or equal to 123 or less than
            or equal 122 (all of a users jobs).
            
            Example 5:
            timeend = "2008-04-5,"
            All jobs that ended after April 5, 2008.
            
            *includesys*: [ bool ]
            Enable/disable returning system jobs (Default is False).

        Returns:

            A list of job objects?
        
        """

        kargs = locals().copy()
        # do not want.
        kargs.pop('includesys')
        kargs.pop('self')

        search = []

        for arg in kargs:
            if kargs[arg]:
                search.append(":".join(( arg, str(kargs[arg]) )) )

        payload = ";".join((search))

        jobres = self.GetJobs(owner_wsid = self._wsid,
                              owner_pw = self._pw,
                              conditions = payload,
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
            Name of a file to save CSV output to (extension will be
            .csv by default). (Default is None).

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
                                        isSystem = issystem)

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
        
        
        return jobid['SubmitExtractJobResult']

    def submit_job(self, qry = "", db = "MyDB", taskname = "pyjob", estimate = 1):
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
                               pw = self._pw,
                               qry = qry,
                               context = db,
                               taskname = taskname,
                               estimate = estimate)

                
        return jobid['SubmitJobResult']

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

    def get_output(self, jobid, path = "", name = ""):
        """
        Download the output created by a submit_extract_job()
        call. The file will be saved in the current working directory
        unless *path* is specified.

        Required arguments:

            *jobid*: [ string ]
            The ID of the submit_extract_job that created output.

        Optional arguments:

            *path*: [ string ]
            Path where the output file should be saved (Default is
            current working directory).

            *name*: [ string ]
            If this parameter is given then the downloaded file will
            be given this filename instead of the default casjobs
            name.

        """
        # Download from an output url returned by get_jobs

        job = self.get_job(jobid = jobid)
        if job.output_loc:
            casfile = urllib2.urlopen(job.output_loc)

        if name:
            fname = name
        else:
            fname = job.output_loc.split("/")[-1]

        if path:
            if os.path.isdir(path):
                if path[-1] != os.path.sep:
                    path = path + os.path.sep
                f = open(path + fname, 'w')
            else:
                print("WARNING: bad path given, file being saved to current working directory")
                f = open(fname, 'w')

        else:
            f = open(fname, 'w')
            
        try:
            f.write(casfile.read())
        except:
            casfile.close()
            f.close()
            print("WARNGING: failed to write output file")

        return fname

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
