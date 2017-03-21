import ftplib
class perform:
    #def __init__(self):
    #    print 'class initialized...'
    #    #self.DataFilePathName = []
    #    #self.BuildFilepaths()
    
    def __init__(self,p_datafilepathname):
        print 'initialized readandloadmanager and strategyandportcodelist.py'
        self.DataFilePathName = p_datafilepathname
        
        self.ReadAndLoad()
        print 'exiting readandloadmanager and strategyandportcodelist.py'

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
            print 'got here !', my_datafilepathname
            filedatetime = os.path.getmtime(my_datafilepathname)
            
            from datetime import datetime
            filedatetime_forsql = datetime.fromtimestamp(filedatetime).strftime('%Y-%m-%d %H:%M:%S')
            
            
            import bs4, sys

            with open(my_datafilepathname, 'r') as f:
                webpage = f.read().decode('utf-8')

            soup = bs4.BeautifulSoup(webpage, "lxml")
            row_id = ''
            fieldnames = {}
            is_dataline = 0
            total_deleted = 0
            total_inserted = 0
            already_ran_delete_statement = 0
            
            for node in soup.find_all('th', attrs={}):     #'style':'display: table-header-group;	mso-number-format:\@;'
                if node.attrs['class'][0] in ['HeaderCellNumeric','HeaderCellString']:
                    fieldnames[len(fieldnames)] = node.string
            for nodeA in soup.find_all('tr', attrs={}):
                print '-----------------------'
                is_dataline = 0
                fieldvalues = {}
                for nodeB in nodeA.find_all('td', attrs={}):
                    #print 'got here!!'
                    #print nodeB.attrs['class'][0]
                    if nodeB.attrs['class'][0] in ['DataCellNumeric','DataCellString']:
                        #print 'got here!!!'
                        if fieldnames[len(fieldvalues)] == 'RowID':
                            is_dataline = 1
                            row_id = nodeB.string
                            print 'RowID:',row_id
                        #print row_id, fieldnames[len(fieldvalues)],'=', nodeB.string
                        fieldvalues[fieldnames[len(fieldvalues)]] = nodeB.string

                if is_dataline == 1:
                    #print 'got here !@'
                    fieldnames_string = ''
                    fieldvalues_string = ''
                
                    for k,v in fieldvalues.items():
                        #print '#',k,v,
                        fieldnames_string = fieldnames_string + k + ','
                        fieldvalues_string = fieldvalues_string + "'" + v + "',"
                    
                    fieldnames_string = fieldnames_string[:-1] 
                    fieldvalues_string = fieldvalues_string[:-1]
                    #print 'fieldnames_string....................'
                    #print fieldnames_string

                    #fieldnames_string = fieldnames_string + 'last_update'
                    #fieldvalues_string = fieldvalues_string + "'" + filedatetime_forsql + "'"
                    #print fieldnames_string
                    #print fieldvalues_string

                    #print row_id
                    #print fieldvalues[fieldnames[0]],fieldvalues[fieldnames[1]],fieldvalues[fieldnames[2]]


                    import pyodbc

                    cnxn = pyodbc.connect(r'DRIVER={SQL Server};SERVER=ipc-vsql01;DATABASE=DataAgg;Trusted_Connection=True;')
                    cursor = cnxn.cursor()
                    if already_ran_delete_statement == 0:
                        cursor.execute("delete from dbo.xanalysisofbenchmarks_managerandstrategyandportcodelist where AsOfDate = ?", fieldvalues['AsOfDate'] )
                        total_deleted = total_deleted + cursor.rowcount
                        print '  ',cursor.rowcount, 'records deleted'
                        cnxn.commit()
                        already_ran_delete_statement = 1
                        
                    insert_sql = "insert into xanalysisofbenchmarks_managerandstrategyandportcodelist("+fieldnames_string+") values ("+fieldvalues_string+")"
                    print insert_sql
                    cursor.execute(insert_sql)
                    procresults['records inserted'] = cursor.rowcount
                    total_inserted = total_inserted + cursor.rowcount
                    print '  ',cursor.rowcount, 'records inserted'
                    cnxn.commit()
                    
            procresults['resultvalue1'] = 'success'
            procresults['total_deleted'] = total_deleted
            procresults['total_inserted'] = total_inserted
            
            
        except Exception,e:
            print type(e)
            print 'there was an error on ' + self.DataFilePathName
            
        self.Results = procresults


    

if __name__=='__main__':
    print 'running ___name___'
    myDataFilePathName = r"//Ipc-vsql01/data/Batches/prod/WatchFolder/incoming/PagesOutput_GetManagerAndStrategyAndPortcodeList_2017-03-02 135329520.xls"
    o = perform(myDataFilePathName)
    print o.Results
    
