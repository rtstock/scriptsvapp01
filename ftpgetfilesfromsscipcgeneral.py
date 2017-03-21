import ftplib
import sys
class perform:
    def __init__(self,remoteftppath):
        print 'ftpgetfilesfromsscipcgeneral.py perform class initialized...'
        print 'passed in parameter set to: ',remoteftppath
        self.ListOfFilepaths = []
        self.BuildFilepaths(remoteftppath)
        
    def set_ListOfFilepaths(self,ListOfFilepaths):
        self._ListOfFilepaths = ListOfFilepaths
    def get_ListOfFilepaths(self):
        return self._ListOfFilepaths
    ListOfFilepaths = property(get_ListOfFilepaths, set_ListOfFilepaths)

    def BuildFilepaths(self,remoteftppath):
        ftp = ftplib.FTP('ftp.sscgateway.com')                                        
        ftp.login('ssc519', 'G2343DRTA')
        ftp.cwd(remoteftppath)
        self.ftp_walk(ftp)


    def ftp_walk(self,ftp):
        from os import path
        #print 'Path:', ftp.pwd()
        dirs = ftp.nlst()
        myPath = ''
        fullpath = ''
        listoffilepaths = []
        for item in (path for path in dirs if path not in ('.', '..')):
            try:
                ftp.cwd(item)
                mypath = ftp.pwd()
                #print 'Changed to', ftp.pwd()
                self.ftp_walk(ftp)
                ftp.cwd('..')
            except Exception, e:
                #print ftp.pwd() + '/' + item
                self.ListOfFilepaths.append(ftp.pwd() + '/' + item)
                #print fullpath
    

if __name__=='__main__':
    print 'running ___name___'

    params = {}
    params['remoteftppath'] = "//USR//SSC519//IPC//ProcOutput"
    for s in sys.argv[1:]:
        print '--',s
        ls = s.split("=")
        params[ls[0]] = ls[1]
        print 'setting',ls[0],'to',ls[1]
    
    myremoteftppath = params['remoteftppath']
    print 'params myremoteftppath',myremoteftppath
    o = perform(myremoteftppath)
    #o = perform("//USR//SSC519//IPC//ProcOutput")
    
    ls = o.ListOfFilepaths
    for f in ls:
        print f
    
    
