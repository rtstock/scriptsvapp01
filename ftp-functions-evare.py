class procedure:
    
    def __init__(self,
           # procname = 'xdeletethis_sylvan'
           # , params = {}
                     ):
        print 'started perform.__init__'
        
    def getfilesfromftp(self, myfileroot):
        import ftplib
        from ftplib import FTP 
        import os

        #File2Send = fullpathtofile 
        #if os.path.exists(File2Send):
        #    print 'found file'
        
        Output_Directory = "//USR//SSC519//evare" 
        
        ftp = FTP("ftp.sscgateway.com")
        
        ftp.login('ssc519', 'G2343DRTA') 

        

        #file = open(File2Send, "rb")
        
        ftp.cwd(Output_Directory)
        
        #List the files in the current directory
        #print "File List:"

        countoffiles = 0
        trav = self.traverse(ftp)
        for remotefilename in trav:
            fileroot = remotefilename[:len(myfileroot)]
            
            if fileroot == myfileroot:
                last4chr = remotefilename[-4:].lower()
                if last4chr == '.csv':
                    print 'found',remotefilename
                    localfullpathname = '\\\\ipc-vsql01\\Data\\Batches\\prod\\WatchFolder\\incoming\\' + remotefilename
                    localfile = open(localfullpathname, "wb")
                    ftp.retrbinary('RETR %s' % remotefilename, localfile.write)
                    #ftp.delete(remotefilename)
                    countoffiles += 1
                    
        return countoffiles





    def traverse(self, ftp, depth=0):
        import ftplib
        """
        return a recursive listing of an ftp server contents (starting
        from the current directory)

        listing is returned as a recursive dictionary, where each key
        contains a contents of the subdirectory or None if it corresponds
        to a file.

        @param ftp: ftplib.FTP object
        """
        if depth > 10:
            return ['depth > 10']
        level = {}
        for entry in (path for path in ftp.nlst() if path not in ('.', '..')):
            try:
                ftp.cwd(entry)
                level[entry] = self.traverse(ftp, depth+1)
                ftp.cwd('..')
            except ftplib.error_perm:
                level[entry] = None
        return level

if __name__ == "__main__":
    import sys
    print 'processing" "if __main__ now:"'
    
    from datetime import datetime, timedelta
    o = procedure()

    today = datetime.today()
    todaystring = datetime.now().strftime("%Y%m%d")

    params={
        "Date8": todaystring
        }
    for s in sys.argv[1:]:
        ls = s.split("=")
        params[ls[0]] = ls[1]
        print 'setting',ls[0],'to',ls[1]
    print 'params',params
    print 'params(Date8)',params['Date8']
    #PershingIPCPrice
    fileroot = 'PershingIPCPrice'+params['Date8']
    numoffiles = o.getfilesfromftp(fileroot)
    print 'fileroot',fileroot,'numoffiles =',numoffiles

    #ComericaIPCPrice
    fileroot = 'ComericaIPCPrice'+params['Date8']
    numoffiles = o.getfilesfromftp(fileroot)
    print 'fileroot',fileroot,'numoffiles =',numoffiles

    #WFAIPCPrice
    fileroot = 'WFAIPCPrice'+params['Date8']
    numoffiles = o.getfilesfromftp(fileroot)
    print 'fileroot',fileroot,'numoffiles =',numoffiles
    
    ## process file using watch folder
    if numoffiles > 0:
        changefilepathname = '\\\\ipc-vsql01\\Data\\Batches\\prod\\WatchFolder\\watch-for-changes-here\\changethis.txt'
        with open(changefilepathname, 'a') as file:
            file.write('ftp-functions-evare '+str(today))
