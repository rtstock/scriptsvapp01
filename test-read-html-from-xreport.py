# -*- coding: utf-8 -*-
"""
Created on Wed Jun 03 10:50:44 2015

@author: justin.malinchak

------------
Description:
------------
Downloads HRFX data for whatever data is available on their website when the process is executed.
The output of this process is a pipe delimited csv and should be used for bulk loading into SQL

"""
def xstr(s):
    try:
        return '' if s is None else str(s)
    except:
        return ''

resultFilepathname = r"//Ipc-vsql01/data/Batches/prod/WatchFolder/incoming/PagesOutput_GetPadPortBenchAsOf_20161124_ADAKAT.xls"
# get and format the modified date
import os.path, time
filedatetime = os.path.getmtime(resultFilepathname)
from datetime import datetime
filedatetime_forsql = datetime.fromtimestamp(filedatetime).strftime('%Y-%m-%d %H:%M:%S')


import bs4, sys

with open(resultFilepathname, 'r') as f:
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
    fieldvalues_string = fieldvalues_string + "'" + xstr(v) + "',"
    print k,v

fieldnames_string = fieldnames_string + 'last_update'
fieldvalues_string = fieldvalues_string + "'" + filedatetime_forsql + "'"

import pyodbc

cnxn = pyodbc.connect(r'DRIVER={SQL Server};SERVER=ipc-vsql01;DATABASE=DataAgg;Trusted_Connection=True;')
cursor = cnxn.cursor()

cursor.execute("delete from dbo.xanalysisofbenchmarks_padportbenchasof_imported where class_last_invested_date = ? and portfolio_ext = ?", fieldvalues['class_last_invested_date'],fieldvalues['portfolio_ext']  )
print cursor.rowcount, 'records deleted'
cnxn.commit() 

cursor.execute("insert into xanalysisofbenchmarks_padportbenchasof_imported("+fieldnames_string+") values ("+fieldvalues_string+")")
print cursor.rowcount, 'records inserted'
cnxn.commit() 

