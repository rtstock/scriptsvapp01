try:
    import os
    path_to_dir = "//ipc-vsql01/data/Batches/prod/WatchFolder/incoming"
    #for fn in os.listdir(path_to_dir):
    #    if os.path.isfile(fn):
    #        print (fn)
    import readandloadperfpadportbenchasof as ralpba
    import readandloadperfpadportasof as ralpa
    import readandloadperfpadbenchasof as ralba
    import readandloadperfpadportdaily as ralpd
    import readandloadmanagerstrategyandportcodelist as ralmsp
    import readandloadmanagerandstrategyandportcodelist as ralmasp
    import readandloadmanagerconsensus as ralmc
    import readandloadaumblock as ralab
    import readandloadMapPortfolioToBenchmark as ralmptb
    import readandloadgetsylvanbenchmarkweights as ralsbw

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
                o = ralpba.perform(datafilepathname)
                print o.Results
                shutil.move(datafilepathname, archivefilepathname)
        
            rootname = 'PagesOutput_GetPPAsOf_'
            rootchars = len(rootname)
            #print file_in_dir[:rootchars]
            if file_in_dir[:rootchars] == rootname:
                datafilepathname = path_to_dir + '/' + file_in_dir
                archivefilepathname = path_to_dir + '/Archive/' + file_in_dir
                print datafilepathname
                o = ralpa.perform(datafilepathname)
                print o.Results
                shutil.move(datafilepathname, archivefilepathname)

            rootname = 'PagesOutput_GetPBAsOf_'
            rootchars = len(rootname)
            #print file_in_dir[:rootchars]
            if file_in_dir[:rootchars] == rootname:
                datafilepathname = path_to_dir + '/' + file_in_dir
                archivefilepathname = path_to_dir + '/Archive/' + file_in_dir
                print datafilepathname
                o = ralba.perform(datafilepathname)
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
                o = ralpd.perform(datafilepathname)
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
                o = ralmsp.perform(datafilepathname)
                print o.Results
                shutil.move(datafilepathname, archivefilepathname)

            rootname = 'PagesOutput_GetManagerAndStrategyAndPortcodeList_'
            rootchars = len(rootname)
            #print file_in_dir[:rootchars]
            if file_in_dir[:rootchars] == rootname:
                datafilepathname = path_to_dir + '/' + file_in_dir
                archivefilepathname = path_to_dir + '/Archive/' + file_in_dir
                print datafilepathname
                o = ralmasp.perform(datafilepathname)
                print o.Results
                shutil.move(datafilepathname, archivefilepathname)

                    
            rootname = 'PagesOutput_GetManagerConsensus_'
            rootchars = len(rootname)
            #print file_in_dir[:rootchars]
            if file_in_dir[:rootchars] == rootname:
                newfilename='x'+file_in_dir
                print file_in_dir
                print newfilename
                
                os.rename(path_to_dir + '/' + file_in_dir,path_to_dir + '/' + newfilename)
                
                datafilepathname = path_to_dir + '/' + newfilename
                archivefilepathname = path_to_dir + '/Archive/' + file_in_dir
                print datafilepathname
                o = ralmc.perform(datafilepathname)
                print o.Results
                if o.Results['status'] == 'success':
                    shutil.move(datafilepathname, archivefilepathname)
                print datafilepathname
                print archivefilepathname
                print 'ok done with that one!!!!!'
                #break

            rootname = 'PagesOutput_GetAUMBlock_'
            rootchars = len(rootname)
            #print file_in_dir[:rootchars]
            if file_in_dir[:rootchars] == rootname:
                newfilename='x'+file_in_dir
                print file_in_dir
                print newfilename
                os.rename(path_to_dir + '/' + file_in_dir,path_to_dir + '/' + newfilename)
                datafilepathname = path_to_dir + '/' + newfilename
                archivefilepathname = path_to_dir + '/Archive/' + file_in_dir
                print datafilepathname
                o = ralab.perform(datafilepathname)
                print o.Results
                if o.Results['status'] == 'success':
                    shutil.move(datafilepathname, archivefilepathname)
    ####
                    
            rootname = 'PagesOutput_MapPortfolioToBenchmark_'
            rootchars = len(rootname)
            #print file_in_dir[:rootchars]
            if file_in_dir[:rootchars] == rootname:
                newfilename='x'+file_in_dir
                print file_in_dir
                print newfilename
                os.rename(path_to_dir + '/' + file_in_dir,path_to_dir + '/' + newfilename)
                datafilepathname = path_to_dir + '/' + newfilename
                archivefilepathname = path_to_dir + '/Archive/' + file_in_dir
                print datafilepathname
                o = ralmptb.perform(datafilepathname)
                print o.Results
                if o.Results['status'] == 'success':
                    shutil.move(datafilepathname, archivefilepathname)
    ####
                    
            rootname = 'PagesOutput_GetSylvanBenchmarkWeights_'
            rootchars = len(rootname)
            #print file_in_dir[:rootchars]
            if file_in_dir[:rootchars] == rootname:
                newfilename='x'+file_in_dir
                print file_in_dir
                print newfilename
                os.rename(path_to_dir + '/' + file_in_dir,path_to_dir + '/' + newfilename)
                datafilepathname = path_to_dir + '/' + newfilename
                archivefilepathname = path_to_dir + '/Archive/' + file_in_dir
                print datafilepathname
                o = ralsbw.perform(datafilepathname)
                print o.Results
                if o.Results['status'] == 'success':
                    shutil.move(datafilepathname, archivefilepathname)
except Exception,e:
    print type(e)
    print 'there was an error in processall-xreport-xls-fromwatchfolderincoming.py'
