#!/usr/bin/env python

import os, sys, time
fileaffected = ""
runprocessflag = []

def files_to_timestamp(path):
    files = [os.path.join(path, f) for f in os.listdir(path)]
    return dict ([(f, os.path.getmtime(f)) for f in files])

if __name__ == "__main__":
    print '-----------------------'
    print 'First arg',sys.argv[0]
    print 'Second arg',sys.argv[1]
    print 'Third arg',sys.argv[2]
    path_to_watch = sys.argv[1]
    program_to_run = sys.argv[2]
    print '+++++++++++++++++++++'
    print '+++++++++++++++++++++'
    print 'NOTICE: THIS IS A WATCHER PROGRAM.'
    print '    Implemented by J. MALINCHAK For Autoloading data to IPC-VSQL01'
    print '+++++++++++++++++++++'
    print '+++++++++++++++++++++'
    print ''
    print "++ Watching....."
    print "     ", path_to_watch
    print "++ On change will run....."
    print "     ", program_to_run
    before = files_to_timestamp(path_to_watch)
    from subprocess import call
    #call("D:\ClientData\Automation Projects\ExtractData\code\bat\test_xdeletethis_sylvan.bat")
    while 1:
        time.sleep (2)
        after = files_to_timestamp(path_to_watch)

        added = [f for f in after.keys() if not f in before.keys()]
        removed = [f for f in before.keys() if not f in after.keys()]
        modified = []

        for f in before.keys():
            if not f in removed:
                if os.path.getmtime(f) != before.get(f):
                    modified.append(f)

        if added:
            print "ok got here 1"
            print "Added: ", ", ".join(added)
            runprocessflag = [1]
            fileaffected = added
        if removed:
            print "Removed: ", ", ".join(removed)
            
        if modified:
            print "ok got here 2"
            print "Modified ", ", ".join(modified)
            runprocessflag = [1]
            fileaffected = modified

        
        #print 'runprocessflag = ', runprocessflag
        if runprocessflag:
            print 'runprocessflag = ', runprocessflag
            print 'file affected: ', fileaffected    
            #call("D:\ClientData\Automation Projects\ExtractData\code\bat\test_xdeletethis_sylvan.bat")
            #call('"D:\\ClientData\\Automation Projects\\ExtractData\\code\\bat\\test_xdeletethis_sylvan.bat"')
            print 'Calling: ', program_to_run

            #call(program_to_run,fileaffected)
            print "***********************", 'This is in Python', fileaffected[0]
            callparm = ''.join(['"', program_to_run,'" "',fileaffected[0],'"'])
            print 'callparm', callparm
            from subprocess import check_call
            check_call(callparm, shell=True)


            #from subprocess import *
            #p = Popen([program_to_run, fileaffected], stdout=PIPE, stderr=PIPE)
            #output, errors = p.communicate()
            #p.wait() # wait for process to terminate
            
            print '*********************'
            print '*********************'
            print 'NOTICE: THIS WAS TRIGGERED FROM A WATCHER IN PYTHON'
            print '*********************'
            print '*********************'
            print ''
            print "++ Watching....."
            print "     ", path_to_watch
            print "++ On change will run....."
            print "     ", program_to_run
            runprocessflag = []
        before = after
