/*
 Assumes GWP scheduler is set to output a daily file 
	prefixed "GWP_Accounts_" 
*/

-- To update the currently configured value for advanced options.

--EXEC sp_configure 'show advanced options', 1

--GO

--RECONFIGURE

--GO

-- To enable the feature to execute a batch file

--EXEC sp_configure 'xp_cmdshell', 1

--GO

--RECONFIGURE

--GO 

-- To execute the batch file

EXEC master..xp_CMDShell 'E:\DATA\Batches\prod\GWP-Extracts-Local-Load\send_email_of_results_batch.bat' 

GO

-- To disable xp_cmdshell

--EXEC sp_configure 'xp_cmdshell', 0

--GO

--RECONFIGURE

--GO

-- To disable advanced options

--EXEC sp_configure 'show advanced options', 0

--GO
