import ftplib
class perform:
    #def __init__(self):
    #    print 'class initialized...'
    #    #self.DataFilePathName = []
    #    #self.BuildFilepaths()
    
    def __init__(self,p_datafilepathname):
        print 'initialized readandloadperfpadportbenchasof.py'
        self.DataFilePathName = p_datafilepathname
        self.ReadAndLoad()
        print 'exiting readandloadperfpadportbenchasof.py'

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
            # get and format the modified date
            import os.path, time
            filedatetime = os.path.getmtime(my_datafilepathname)
            from datetime import datetime
            filedatetime_forsql = datetime.fromtimestamp(filedatetime).strftime('%Y-%m-%d %H:%M:%S')


            import bs4, sys

            with open(my_datafilepathname, 'r') as f:
                webpage = f.read().decode('utf-8')

            soup = bs4.BeautifulSoup(webpage, "lxml")

            fieldnames = {}
            for node in soup.find_all('th', attrs={}):     #'style':'display: table-header-group;	mso-number-format:\@;'
                if node.attrs['class'][0] in ['HeaderCellNumeric','HeaderCellString']:
                    fieldnames[len(fieldnames)] = node.string

            fieldvalues = {}
            for node in soup.find_all('td', attrs={}):
                if node.attrs['class'][0] in ['DataCellNumeric','DataCellString']:
                    fieldvalues[fieldnames[len(fieldvalues)]] = node.string
                
            fieldnames_string = ''
            fieldvalues_string = ''
            for k,v in fieldvalues.items():
                fieldnames_string = fieldnames_string + k + ','
                fieldvalues_string = fieldvalues_string + "'" + self.xstr(v) + "',"
                #print k,v

            fieldnames_string = fieldnames_string + 'last_update'
            fieldvalues_string = fieldvalues_string + "'" + filedatetime_forsql + "'"

            procresults['class_last_invested_date'] = fieldvalues['class_last_invested_date']
            procresults['portfolio_ext'] = fieldvalues['portfolio_ext']
            print '  ',fieldvalues['class_last_invested_date'],fieldvalues['portfolio_ext'], filedatetime_forsql

            import pyodbc

            cnxn = pyodbc.connect(r'DRIVER={SQL Server};SERVER=ipc-vsql01;DATABASE=DataAgg;Trusted_Connection=True;')
            cursor = cnxn.cursor()

            cursor.execute("delete from dbo.xanalysisofbenchmarks_padportbenchasof_imported where class_last_invested_date = ? and portfolio_ext = ?", fieldvalues['class_last_invested_date'],fieldvalues['portfolio_ext']  )
            procresults['records deleted'] = cursor.rowcount
            print '  ',procresults['records deleted'], 'records deleted'
            cnxn.commit() 

            cursor.execute("insert into xanalysisofbenchmarks_padportbenchasof_imported("+fieldnames_string+") values ("+fieldvalues_string+")")
            procresults['records inserted'] = cursor.rowcount
            print '  ',procresults['records inserted'], 'records inserted'
            cnxn.commit() 
        except:
            print 'there was an error on ' + self.DataFilePathName
        self.Results = procresults


    

if __name__=='__main__':
    print 'running ___name___'
    myDataFilePathName = r"//Ipc-vsql01/data/Batches/prod/WatchFolder/incoming/PagesOutput_GetPadPortBenchAsOf_20161124_ADAKAT.xls"
    o = perform(myDataFilePathName)
    #o.DataFilePathName = r"//Ipc-vsql01/data/Batches/prod/WatchFolder/incoming/PagesOutput_GetPadPortBenchAsOf_20161124_ADAKAT.xls"
    #o.ReadAndLoad()
    print o.Results
    
