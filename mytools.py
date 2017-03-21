# -*- coding: utf-8 -*-
"""
Created on Sat Dec  6 10:57:01 2014

@author: jmalinchak
"""
class general:
    def make_sure_path_exists(self,path):
        import errno
        import os
        try:
            if not os.path.exists(path):
                os.makedirs(path)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise
                

    def make_sure_filepath_exists(filepath):
        import errno
        import os
        try:
            print(filepath)
            path = os.path.dirname(os.path.abspath(filepath))
            print(path)
            os.makedirs(path)
            
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise

class get_from_optionsymbol: 
    def expirationdate(self,optionsymbol):
        #print(optionsymbol)
        ##print('a-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb')
        s1 = optionsymbol[-15:-9]
        #s2 = s1[:-9]
        #print(s1)
        from datetime import datetime
        d1 = datetime.strptime(s1, '%y%m%d')
        return d1
    def strike(self,optionsymbol):
        s1 = optionsymbol[-8:]
        s2 = s1[:5] + '.' + s1[5:]
        ##print('a-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')
        return float(s2)
    def optiontype(self,optionsymbol):
        optiontype_1 = optionsymbol[-9]
        return optiontype_1
    def symbol(self,optionsymbol):
        #s1 = len(optionsymbol)
        s2 = optionsymbol[:-15]
        return s2
        
class mystrings:
    def datetimenormal():
        import datetime
        sdatetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return sdatetime
        
    def appendnormaldateddirectorybasedoncurrenttime15(root):
        import datetime
        sdate = datetime.datetime.now().strftime("%Y-%m-%d")
        stime = datetime.datetime.now().strftime("%H:%M:%S")
        shour = stime.split(':')[0]
        sminute = stime.split(':')[1]
        #print(int(sminute))
        sminutenormal = '0'
        if int(sminute) < 15:
            sminutenormal = '15'
        if int(sminute) >= 15 and int(sminute) < 30:
            sminutenormal = '30'
        if int(sminute) >= 30 and int(sminute) < 45:
            sminutenormal = '45'
        if int(sminute) >= 45 and int(sminute) < 60:
            sminutenormal = '60'
        
        #root = "C:\\Documents and Settings\\jmalinchak\\My Documents\\My Python\\Active\\downloads"
        final = root + "\\" + sdate + "\\" + shour + "\\" + sminutenormal
        return final
    def appendnormaldateddirectorybasedondatetimeparameter(root,datetimeparameter):
       
        sdate = datetimeparameter.strftime("%Y-%m-%d")
        stime = datetimeparameter.strftime("%H:%M:%S")
        shour = stime.split(':')[0]
        sminute = stime.split(':')[1]
        print('======== =======================================')
        print('========',datetimeparameter,'converts to minute',int(sminute))
        
        sminutenormal = '0'
        if int(sminute) < 15:
            sminutenormal = '15'
        if int(sminute) >= 15 and int(sminute) < 30:
            sminutenormal = '30'
        if int(sminute) >= 30 and int(sminute) < 45:
            sminutenormal = '45'
        if int(sminute) >= 45 and int(sminute) < 60:
            sminutenormal = '60'
        
        #root = "C:\\Documents and Settings\\jmalinchak\\My Documents\\My Python\\Active\\downloads"
        final = root + "\\" + sdate + "\\" + shour + "\\" + sminutenormal
        print(final)
        print('======== =======================================')
        return final
    def ConvertDatetime14():
        import datetime
        s = str(datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
        return s
        
    def ConvertStringToDate(MyString):
        import datetime

        minyear = 1900
        maxyear = 2060
        
        mydate = MyString
        
        dateparts = mydate.split('-')
#        print(dateparts[0])
#        print(dateparts[1])
#        print(dateparts[2])
        try:
            if len(dateparts) != 3:
               raise ValueError("Invalid date format")
            if int(dateparts[0]) > maxyear or int(dateparts[0]) <= minyear:
               raise ValueError("Year out of range")
            
            dateobj = datetime.date(int(dateparts[0]),int(dateparts[1]),int(dateparts[2]))
            #print(str(dateobj)) #str(dateobj
            return dateobj
        except:
            return datetime.date(1900,1,1)
#                                          
#                                          
#C:\\Documents and Settings\\jmalinchak\\My Documents\\My Python\\Active\\inputs\\Symbols.txt
#C:\\Documents and Settings\\jmalinchak\\My Documents\\My Python\\Active\\inputs\\Expirations.txt
