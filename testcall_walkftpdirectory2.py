import ftplib
def ftpwalk(ftp):
        from os import path
        #print 'Path:', ftp.pwd()
        dirs = ftp.nlst()
        fullpath = ''
        listoffilepaths = []
        for item in (path for path in dirs if path not in ('.', '..')):
            try:
                ftp.cwd(item)
                mypath = ftp.pwd()
                #print 'Changed to', ftp.pwd()
                ftp_walk(ftp)
                ftp.cwd('..')
            except Exception, e:
                fullpath = ftp.pwd() + '/' + item
                listoffilepaths.append(fullpath)
                print fullpath
                #print fullpath
        return listoffilepaths

ftp = ftplib.FTP('ftp.sscgateway.com')                                        
ftp.login('ssc519', 'G2343DRTA')
ftp.cwd("//USR//SSC519//IPC" )
ls = ftpwalk(ftp)
print '------------------------------------'
for f in ls:
    print '*',f
        
