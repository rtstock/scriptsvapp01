import os
path_to_dir = "//ipc-vsql01/data/Batches/prod/WatchFolder/incoming"
#for fn in os.listdir(path_to_dir):
#    if os.path.isfile(fn):
#        print (fn)
import readandloadperfpadportbenchasof as rppbasof
import readandloadperfpadportasof as rppasof
import readandloadperfpadbenchasof as rpbasof
import readandloadperfpadportdaily as rppdaily
import readandloadmanagerstrategyandportcodelist as rmsapl
import readandloadmanagerconsensus as ralmc
import shutil
files_in_dir = os.listdir(path_to_dir)
for file_in_dir in files_in_dir:
    if file_in_dir[-4:].lower() == '.xls':
        rootname = 'PagesOutput_GetPadPortBenchAsOf_'
        rootchars = len(rootname)
        #print file_in_dir[:rootchars]
        if file_in_dir[:rootchars] == rootname:
            datafilepathname = path_to_dir + '/' + file_in_dir
            archivefilepathname = path_to_dir + '/Archive/' + file_in_dir
            print datafilepathname
            o = rppbasof.perform(datafilepathname)
            print o.Results
            shutil.move(datafilepathname, archivefilepathname)
    
        rootname = 'PagesOutput_GetPPAsOf_'
        rootchars = len(rootname)
        #print file_in_dir[:rootchars]
        if file_in_dir[:rootchars] == rootname:
            datafilepathname = path_to_dir + '/' + file_in_dir
            archivefilepathname = path_to_dir + '/Archive/' + file_in_dir
            print datafilepathname
            o = rppasof.perform(datafilepathname)
            print o.Results
            shutil.move(datafilepathname, archivefilepathname)

        rootname = 'PagesOutput_GetPBAsOf_'
        rootchars = len(rootname)
        #print file_in_dir[:rootchars]
        if file_in_dir[:rootchars] == rootname:
            datafilepathname = path_to_dir + '/' + file_in_dir
            archivefilepathname = path_to_dir + '/Archive/' + file_in_dir
            print datafilepathname
            o = rpbasof.perform(datafilepathname)
            print o.Results
            shutil.move(datafilepathname, archivefilepathname)

        #PagesOutput_GetPadPortDaily_2016-12-02 104433477.xls
        rootname = 'PagesOutput_GetPadPortDaily_'
        rootchars = len(rootname)
        #print file_in_dir[:rootchars]
        if file_in_dir[:rootchars] == rootname:
            datafilepathname = path_to_dir + '/' + file_in_dir
            archivefilepathname = path_to_dir + '/Archive/' + file_in_dir
            print datafilepathname
            o = rppdaily.perform(datafilepathname)
            print o.Results
            shutil.move(datafilepathname, archivefilepathname)

        #readandloadmanagerstrategyandportcodelist.py
        #PagesOutput_GetManagerStrategyAndPortcodeList_2016-12-08 130507100
        rootname = 'PagesOutput_GetManagerStrategyAndPortcodeList_'
        rootchars = len(rootname)
        #print file_in_dir[:rootchars]
        if file_in_dir[:rootchars] == rootname:
            datafilepathname = path_to_dir + '/' + file_in_dir
            archivefilepathname = path_to_dir + '/Archive/' + file_in_dir
            print datafilepathname
            o = rmsapl.perform(datafilepathname)
            print o.Results
            shutil.move(datafilepathname, archivefilepathname)

		
        rootname = 'PagesOutput_GetManagerConsensus_'
        rootchars = len(rootname)
        #print file_in_dir[:rootchars]
        if file_in_dir[:rootchars] == rootname:
            datafilepathname = path_to_dir + '/' + file_in_dir
            archivefilepathname = path_to_dir + '/Archive/' + file_in_dir
            print datafilepathname
            o = ralmc.perform(datafilepathname)
            print o.Results
            shutil.move(datafilepathname, archivefilepathname)