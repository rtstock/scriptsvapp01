import ftplib
class perform:
    def __init__(self):
        print 'class initialized...'
        self.ListOfFilepaths = []
        self.BuildFilepaths()
        
    def set_ListOfFilepaths(self,ListOfFilepaths):
        self._ListOfFilepaths = ListOfFilepaths
    def get_ListOfFilepaths(self):
        return self._ListOfFilepaths
    ListOfFilepaths = property(get_ListOfFilepaths, set_ListOfFilepaths)

    def BuildFilepaths(self):
        ftp = ftplib.FTP('ftp.sscgateway.com')                                        
        ftp.login('ssc519', 'G2343DRTA')
        ftp.cwd("//USR//SSC519//IPC//Performance" )
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
    o = perform()
    
    ls = o.ListOfFilepaths
    for f in ls:
        print f
    
    
