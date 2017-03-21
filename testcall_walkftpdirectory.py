import ftplib
import os
def ftp_walk(ftp):
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
            print 'Changed to', ftp.pwd()
            ftp_walk(ftp)
            ftp.cwd('..')
        except Exception, e:
            fullpath = ftp.pwd() + '/' + item
            listoffilepaths.append(ftp.pwd() + '/' + item)
            mydirname = os.path.dirname(fullpath)
            print fullpath
            print mydirname
    return listoffilepaths
ftp = ftplib.FTP('ftp.sscgateway.com')                                        
ftp.login('ssc519', 'G2343DRTA')
ftp.cwd("//USR//SSC519//IPC//Performance" )
ls = ftp_walk(ftp)
print ' +++++++++++++++++++++++++++++++++++++++++'

