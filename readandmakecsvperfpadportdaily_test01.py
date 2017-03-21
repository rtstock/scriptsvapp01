# -*- coding: utf-8 -*-
import ftplib
import re
class perform:
    #def __init__(self):
    #    print 'class initialized...'
    #    #self.DataFilePathName = []
    #    #self.BuildFilepaths()
    
    def __init__(self,p_datafilepathname):
        print 'initialized readandloadperfpadportasof.py'
        self.DataFilePathName = p_datafilepathname
        
        self.ReadAndLoad()
        print 'exiting readandloadperfpadportasof.py'

    def set_DataFilePathName(self,DataFilePathName):
        self._DataFilePathName = DataFilePathName
    def get_DataFilePathName(self):
        return self._DataFilePathName
    DataFilePathName = property(get_DataFilePathName, set_DataFilePathName)

    def set_Results(self,Results):
        self._Results = Results
    def get_Results(self):
        return self._Results
    Results = property(get_Results, set_Results)
    
    def xstr(self,s):
        try:
            return '' if s is None else str(s)
        except:
            return ''

    def ReadAndLoad(self):
        procresults = {}
        try:
            
            my_datafilepathname = self.DataFilePathName #r"//Ipc-vsql01/data/Batches/prod/WatchFolder/incoming/PagesOutput_GetPadPortBenchAsOf_20161124_ADAKAT.xls"
            #//Ipc-vsql01/data/Batches/prod/WatchFolder/incoming/PreparedFiles
            # get and format the modified date
            
            import os.path, time
            print 'got here !', my_datafilepathname
            filedatetime = os.path.getmtime(my_datafilepathname)
            
            from datetime import datetime
            filedatetime_forsql = datetime.fromtimestamp(filedatetime).strftime('%Y-%m-%d %H:%M:%S')
            
            
            import bs4, sys

            with open(my_datafilepathname, 'r') as f:
                webpage = f.read().decode('utf-8')

            soup = bs4.BeautifulSoup(webpage, "lxml")
            vlist = []
            import csv
            i = 0
            with open("//Ipc-vsql01/data/Batches/prod/WatchFolder/incoming/PreparedFiles/myfile2.csv","w") as csvfile:
                out = csv.writer(csvfile, delimiter='~',quoting=csv.QUOTE_ALL,lineterminator = '\n')
            
                for node in soup.find_all('th', attrs={}):     #'style':'display: table-header-group;	mso-number-format:\@;'
                    if node.attrs['class'][0] in ['HeaderCellNumeric','HeaderCellString']:
                        vlist.append(node.string)
                out.writerow(vlist)
                for nodeA in soup.find_all('tr', attrs={}):
                    print '-----------------------'
                    vlist = []
                    #i += 1
                    #vlist.append(i)
                    for nodeB in nodeA.find_all('td', attrs={}):
                        #print 'got here!!'
                        #print nodeB.attrs['class'][0]
                        if nodeB.attrs['class'][0] in ['DataCellNumeric','DataCellString']:
                            prepared_value = nodeB.string
                            
                            if prepared_value == None:
                                prepared_value = ''
                            
                            prepared_value = prepared_value.encode('utf-8').strip()
                            prepared_value = re.sub(r'[\xc2\x99]'," ",prepared_value)
                            prepared_value = prepared_value.rstrip()
                            prepared_value = prepared_value.lstrip()
                            vlist.append(prepared_value)
                    print vlist
                    if len(vlist) > 0:
                        out.writerow(vlist)
                        csvfile.flush()
            csvfile.close()

            
            
        except Exception,e:
            print "Here is your error: ",e
            print type(e)
            print 'there was an error on ' + self.DataFilePathName
            
        self.Results = procresults


    

if __name__=='__main__':
    print 'running ___name___'
    myDataFilePathName = r"//Ipc-vsql01/data/Batches/prod/WatchFolder/incoming/xPagesOutput_GetPadPortDaily_2016-12-28 072828340 - Copy.xls"
    o = perform(myDataFilePathName)
    #o.DataFilePathName = r"//Ipc-vsql01/data/Batches/prod/WatchFolder/incoming/xPagesOutput_GetPadPortDaily_2016-12-28 072828340 - Copy.xls"
    #o.ReadAndLoad()
    print o.Results
    
