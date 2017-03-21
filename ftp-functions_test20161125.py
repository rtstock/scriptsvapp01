# This codesheet should "walk" the source ftp directories looking for files.
class procedure:
    
    def __init__(self,
           # procname = 'xdeletethis_sylvan'
           # , params = {}
                     ):
        print 'started perform.__init__'
        
    def _execprocedure(self,session, procname, params):
        sql_params = ",".join(["@{0}={1}".format(name, value) for name, value in params.items()])
        sql_string = """
            DECLARE @return_value int;
            EXEC    @return_value = [dbo].[{procname}] {params};
            SELECT 'Return Value' = @return_value;   
        """.format(procname=procname, params=sql_params)
        #
        print '=================='
        print sql_string
        print '=================='
        return session.execute(sql_string).fetchall()


    
    def execute(self,server,database,procname,params):
        print 'server', server
        print 'database', database
        print 'procname', procname
        print 'params', params

        import urllib
        import sqlalchemy
        connection_string = 'DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=ssc519devpages;PWD=P@ges76!123'
        connection_string = urllib.quote_plus(connection_string) 
        connection_string = "mssql+pyodbc:///?odbc_connect=%s" % connection_string

        #create_engine
        engine = sqlalchemy.create_engine(connection_string, connect_args={'timeout': 600})
        
        #create connection and cursor
        connection = engine.raw_connection()
        cursor = connection.cursor()

        #create session
        from sqlalchemy.orm import sessionmaker
        Session = sessionmaker(bind=engine,autocommit=True)
        # Session is a class
        session = Session()
        # now session is a instance of the class Session

        results = self._execprocedure(session, procname, params)
        #results = self._execprocedure2(cursor, procname, params)
        
        #import csv
        import unicodecsv as csv
        import datetime
        sdatetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S").replace(':','')
        
        import mytools
        import os
        resultfilePath = "D:\ClientData\Automation Projects\ExtractData\output\procoutput " + procname + " " + sdatetime + ".csv "
        resultFile = open(resultfilePath,'wb')
        wr = csv.writer(resultFile, dialect='excel')
        try:
            #wr.writerow(results.headers)
            wr.writerows(results)
            
            
        except Exception as e:
            print '*** you better write an error log ***'
            print e.__doc__
            wr.writerows(e.message)
        if os.path.exists(resultfilePath):
            print 'file exists: ',resultfilePath
        else:
            print 'file not created: ',resultfilePath
        
        print 'done'
        cursor.close()
        connection.commit()

        return resultfilePath
        #for row in results:
        #    for col in row:
        #        print col
        #print type(results)

    def putfiletoftp(self,fullpathtofile):
        import ftplib
        from ftplib import FTP 
        import os
        File2Send = fullpathtofile 
        if os.path.exists(File2Send):
            print 'found file'
        Output_Directory = "//USR//SSC519//IPC" 

    
        
        ftp = FTP("ftp.sscgateway.com")
        ftp.login('ssc519', 'G2343DRTA') 
        
        file = open(File2Send, "rb")
        
        ftp.cwd(Output_Directory)
        
        ftp.storbinary('STOR ' + os.path.basename(File2Send), file) 
        
        ftp.quit() 
        file.close() 
        print "File transfered" 

    def getfilesfromftp(self, fileroot, localfolder):
        try:
            import ftplib
            from ftplib import FTP 
            import os

            #File2Send = fullpathtofile 
            #if os.path.exists(File2Send):
            #    print 'found file'
            
            Output_Directory = "//USR//SSC519//IPC" 

            ftp = FTP("ftp.sscgateway.com")
            ftp.login('ssc519', 'G2343DRTA') 

            

            #file = open(File2Send, "rb")
            
            ftp.cwd(Output_Directory)
            
            #List the files in the current directory
            #print "File List:"

            countoffiles = 0
            #countoffiles += 1
            trav = self.traverse(ftp)
            print 'xxxxxxxxx length of trav',len(trav)
            for ls in trav:

                print '^^^^^ remotefilename', remotefilename
                    
                fileroot = remotefilename[:11]

                if fileroot == 'pagesoutput':
                    print remotefilename
                    localfullpathname = '\\\\ipc-vsql01\\Data\\Batches\\prod\\WatchFolder\\incoming\\' + remotefilename
                    localfile = open(localfullpathname, "wb")
                    print 'retrieving: ',remotefilename
                       
                    print '    success'
                    ftp.delete(remotefilename)
                    countoffiles += 1
                    #ftp.retrbinary('RETR Readme', gFile.write)
            return countoffiles

            #files = ftp.dir()
            #for fx in trav:
            #    print 'zzzzzzz '

            #Get the readme file
            #ftp.cwd("/pub")
            #gFile = open("readme.txt", "wb")
            #ftp.retrbinary('RETR Readme', gFile.write)
            #gFile.close()
            #ftp.quit()
        
            #Print the readme file contents
            #print "\nReadme File Output:"
            #gFile = open("readme.txt", "r")
            #buff = gFile.read()
            #print buff
            #gFile.close()
            #ftp.retrbinary('RETR %s' % filename, file.write)
            
        except Exception as e:
            print '*** you better write an error log ***'
            print e.__doc__


        
    def traverse(self, ftp, depth=0,myfiles=[]):
        import ftplib
        """
        return a recursive listing of an ftp server contents (starting
        from the current directory)

        listing is returned as a recursive dictionary, where each key
        contains a contents of the subdirectory or None if it corresponds
        to a file.

        @param ftp: ftplib.FTP object
        """
        files = []
        if depth > 20:
            return ['depth > 10']
        level = {}
        vfile = ''
        print '--------  begin traverse ------------'
        for entry in (path for path in ftp.nlst() if path not in ('.', '..')):
            
            try:
                #print 'file A',depth,entry,entry[-4:]
                ftp.cwd(entry)
                level[entry] = self.traverse(ftp, depth+1,files)
                print 'file B',depth, 'numofiles',len(files)
                if not entry[-4:].lower() == '.xls':
                    folder = entry
                    print '+',folder, 'numoffile:',len(files)
                    for vfile in files:
                        print '++',vfile
                    #files = []
                ftp.cwd('..')
            except ftplib.error_perm:
                if entry[-4:].lower() == '.xls':
                    print 'file C',depth,entry,entry[-4:].upper()
                    files.append(entry)
                    print 'count of files',len(files)
                print '  except'
                print '@@@@@ entry =',entry
                level[entry] = files
        return level,files

if __name__ == "__main__":
    import sys
    print 'starting python script: ftp-functions.py'
    print 'processing if __main__ == '
    print 'arg list',sys.argv[1:]
    server='ssc519devsql'
    database='SSC519Client' #default
    procname='xdeletethis_holdings2' #default
    
    from datetime import datetime, timedelta
    today = datetime.today()
    yesterday = today - timedelta(1)
    twodaysago = today - timedelta(2)
    trimmedtwodaysago = str(twodaysago).split(' ')[0]
    
    params={
        "AsOfDate": '"' + trimmedtwodaysago + '"'
        #'AsOfDate': '"2016-04-17"'
        }
    for s in sys.argv[1:]:
        ls = s.split("=")

        if ls[0]=='procname':
            procname=ls[1]
        elif ls[0]=='database':
            database=ls[1]
        elif ls[0]=='server':
            server=ls[1]
            print 'server set to', server
        elif ls[0]=='timeout':
            timeout=ls[1]
            print 'timeout set to', timeout
        else:
            params[ls[0]] = ls[1]
            print 'setting',ls[0],'to',ls[1]
    o = procedure()
    numoffiles = o.getfilesfromftp('xyx','xyx')
    # change the changethis.txt file
    #M:\Batches\prod\WatchFolder\watch-for-changes-here\changethis.txt
    print 'numoffiles =',numoffiles
    #if numoffiles > 0:
    
    try:
        print 'attempting to write to changethis.txt.  Waiting 5 secs...'
        import time
        time.sleep(10) # delays for 10 seconds
        changefilepathname = '\\\\ipc-vsql01\\Data\\Batches\\prod\\WatchFolder\\watch-for-changes-here\\changethis.txt'
        with open(changefilepathname, 'a') as file:
            linetowrite = 'ftp_functions.py ' + str(today)
            file.write(linetowrite)
            print 'successfully wrote line:',linetowrite
    except Exception as e:
        print(e)
        print 'this error occurred attempting to write to changethis.txt'
