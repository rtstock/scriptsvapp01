class perform:
    def __init__(self,
            procname = 'xdeletethis_sylvan'
            , params = {}
                     ):
        
        
        self.runprocess(
            myconnectstring,procname, params
            )
        
    def _execprocedure(self,session, procname, params):
        sql_params = ",".join(["'{0}={1}".format(name, value) for name, value in params.items()])
        sql_string = """
            DECLARE @return_value int;
            EXEC    @return_value = [dbo].[{procname}] {params};
            SELECT 'Return Value' = @return_value;
        """.format(procname=procname, params=sql_params)

        return session.execute(sql_string).fetchall()
    
    def runprocess(self,myconnectstring,procname,params):
        print 'procname',procname
        print 'params',params

        import urllib
        import sqlalchemy
        connection_string = 'DRIVER={SQL Server};SERVER=ssc519sql\inst1;DATABASE=ssc519sylvan_oper;UID=ssc519sylvan;PWD=P@ges76!123'
        connection_string = urllib.quote_plus(connection_string) 
        connection_string = "mssql+pyodbc:///?odbc_connect=%s" % connection_string

        #create_engine
        engine = sqlalchemy.create_engine(connection_string, connect_args={'timeout': 45})
        from sqlalchemy.orm import sessionmaker

        Session = sessionmaker(bind=engine)
        # Session is a class
        session = Session()
        # now session is a instance of the class Session

        #params = {
            #'AsOfDate': '"2016-02-29"'
        #}
        results = self._execprocedure(session, procname, params)
        #results =  (session,'xdeletethis_sylvan',params)

        import csv
        import datetime
        sdatetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S").replace(':','')
        
        import mytools
        import os
        resultfilePath = "D:\ClientData\Automation Projects\ExtractData\output\output " + sdatetime + ".csv "
        resultFile = open(resultfilePath,'wb')
        wr = csv.writer(resultFile, dialect='excel')
        wr.writerows(results)
        
        if os.path.exists(resultfilePath):
            print 'file exists: ',resultfilePath
        else:
            print 'file not created: ',resultfilePath
        
        print 'done'
        #for row in results:
        #    for col in row:
        #        print col
        #print type(results)

        
if __name__ == "__main__":
    import sys
    #print sys.argv[1:]
    myconnectstring='DRIVER={SQL Server};SERVER=ssc519sql\inst1;DATABASE=ssc519sylvan_oper;UID=ssc519sylvan;PWD=P@ges76!123' #default
    '''
    exec syl_pad_last_n_risk_stats '5226||','','5','1976||141||','2015-06-30T00:00:00','1',0,'111','','','','0','3Y','5||15||108||124||125||',0
    '''
    procname='syl_pad_last_n_risk_stats' #default
    params={
'p_portfolios':'5226||'
, 'p_attrib_model_id':''
, 'p_class_scheme_id':'5'
, 'p_indexes':'1976||141||'
, 'p_effective_date':'2015-06-30T00:00:00'
, 'p_auv_flavour_key':'1'
, 'p_weight_method_type':'0'
, 'p_risk_free_idx':'111'
, 'p_target_rate1':''
, 'p_target_rate2':''
, 'p_target_rate3':''
, 'p_UsePrimaryIndex':'0'
, 'p_periods':'3Y'
, 'p_measures':'5||15||108||124||125||'
, 'p_report_type':'0'

        }
    for s in sys.argv[1:]:
        ls = s.split("=")

        if ls[0]=='procname':
            procname=ls[1]
        elif ls[0]=='myconnectstring':
            myconnectstring=ls[1]
        else:
            params[ls[0]] = ls[1]
            
    o = perform(procname,params)
    #main(sys.argv[1:])
