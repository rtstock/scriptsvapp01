class perform:
    import os
    import csv
    import re
    import mytools
    otools = mytools.general()
    import config
    def __init__(self,
           # procname = 'xdeletethis_sylvan'
           # , params = {}
                     ):
        print 'started perform.__init__'

    def readfile(self, localunprocessedcsvpathname): #filebasename, localfolder, 
        try:
            checkingon = 0
            mycolumn = -1
            print 'got here'
            with open(localunprocessedcsvpathname, 'r') as f:
                reader = csv.reader(f, dialect='excel', delimiter='\t')
                for row in reader:
                    #print len(row),row, 'xxxxxxxx'
                    if 'ToWorst' in str(row):
                        if 'Value' in str(row):
                            print 'found ToWorst *******************'
                            mycharindex =  [ (i.start(), i.end()) for i in re.finditer('    Value', str(row))]
                            mycharindex_string = str(mycharindex[0])
                            mycharindex_string = mycharindex_string.replace('(','')
                            mycharindex_string = mycharindex_string.replace(')','')
                            mycharindex_string = mycharindex_string.replace(' ','')
                            mycharindex_start = int(mycharindex_string.split(',')[0])
                            mycharindex_end = int(mycharindex_string.split(',')[1])
                            print mycharindex_start,mycharindex_end,'kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk'
                        
                        checkingon = 1

       
                    elif checkingon == 1:
                        mymarketvalue = str(row)[mycharindex_start:mycharindex_end]
                        onlydigits = mymarketvalue.replace(' ','').replace('.','').replace('-','')

                        if checkingon == 1:
                            testsection1 = str(row)[2:len('  Financial Institutio')][:2]
                            #print 'testsection1',testsection1,'11111111111111111111111111111111111111111111111'
                            if testsection1.strip() != '':
                                mysection1 = str(row)[2:len('  Financial Institutio')].strip()
                                mysection1 = mysection1.replace('[','')
                                mysection1 = mysection1.replace(']','')
                                mysection1 = mysection1.replace('\'','')
                                #print mysection1, '@@@@@@@@@@@@@@ Section 1 @@@@@@@@@@@@@@@@'
        
                            testsection2 = str(row)[2:len('  Financial Institutio')][:4]
                            #print 'testsection2',testsection2,'222222222222222222222222222222222222222222222222'
                            if testsection2.strip() != '':
                                mysection2 = str(row)[2:len('  Financial Institutio')].strip()
                                mysection2 = mysection2.replace('[','')
                                mysection2 = mysection2.replace(']','')
                                mysection2 = mysection2.replace('\'','')
                                #print mysection2, '@@@@@@@@@@@@@@ Section 2 @@@@@@@@@@@@@@@@'                  
                          
                      
                      
                        if 'INDEX RESULTS' in str(row):
                            break
                        if len(str(row)) > mycharindex_end:
                            #print str(row)[2:21],str(row)[22:31],str(row)[32:38],mymarketvalue,filename,'|||||',row[0], 'xxxxxxxx', str(row)[32:39]
                            issuename = str(row)[2:len('  Financial Institutio')]
                            issuename = issuename.replace('[','')
                            issuename = issuename.replace(']','')
                            issuename = issuename.replace('\'','')
                            #if not issuename[:2] == '  ':
                            if mymarketvalue.strip().replace('-','').replace('.','').isdigit() == True:
                                if mysection1 == 'U.S. Aggregate' and mysection2 == 'U.S. Aggregate':
                                    #print mysection1,mysection2,mymarketvalue.strip(),filename
                                    mydict[len(mydict)] = {'source':'barclays','category':'agg','mysection1':mysection1,'mysection2':mysection2,'issue':issuename.strip(),'marketvalue':mymarketvalue.strip(),'filename':filename}

            
            
            
            ## #####################
            ## Moves processed files
            import os
            localprocessedcsvpathname = os.path.join(localprocessedfolder,filename) #'E:\Batches\development\projects\Investment Strategy\ETL\Uploads\Ready'.encode('string_escape')
            #if os.path.exists(localprocessedcsvpathname):
            #    os.remove(localprocessedcsvpathname)
            #shutil.move(localunprocessedcsvpathname, localprocessedfolder)
            ## #####################
            
        except Exception as e:
            print '*** you better write an error log ***'
            print e.__doc__



if __name__ == "__main__":
    import sys
    try:
        o = perform()
        o.readfile('C:\Batches\AutomationProjects\Investment Strategy\ETL\Downloads\Unprocessed\barclays\20170314.agg')
    except Exception as e:
        print(e)
        print 'this error occurred attempting to write to changethis.txt'
