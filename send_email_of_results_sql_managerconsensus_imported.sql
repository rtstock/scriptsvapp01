sp_CONFIGURE 'show advanced', 1
GO
RECONFIGURE
GO
sp_CONFIGURE 'Database Mail XPs', 1
GO
RECONFIGURE
GO
USE msdb
GO
declare @date_ManagerConsensus datetime
declare @file_ManagerConsensus varchar(100)



-- select MAX(Last_Update) from DataAgg..GWP_Accounts 

select @date_ManagerConsensus = MAX(Last_Update) from DataAgg.dbo.managerconsensus_imported 
select @file_ManagerConsensus = MAX(SourceFileName) from DataAgg.dbo.managerconsensus_imported 
set @file_ManagerConsensus = isnull(@file_ManagerConsensus,'no file found')


declare @datestring_ManagerConsensus varchar(100)
set @datestring_ManagerConsensus = isnull(convert(varchar(23),@date_ManagerConsensus),'no date found')

declare @BodyText varchar(max)
set  @BodyText ='Database table automatic update notice:'
	+ CHAR(10) 
	+ CHAR(10) 
	+ '  ' + char(149) + '  ' + @@SERVERNAME + '.DataAgg.dbo.managerconsensus_imported updated ' + convert(varchar(23),@date_ManagerConsensus) + ' from file: ' + @file_ManagerConsensus
	+ CHAR(10) 
	+ CHAR(10)
	+'Source: C:\Batches\AutomationProjects\Watcher\code\bat\send_email_of_results_sql_managerconsensus_imported.sql'
print @BodyText

EXEC sp_send_dbmail @profile_name='IPC Mail Profile',
@recipients='justin.malinchak@ipcanswers.com;',
@subject='Job: ManagerConsensus (Daily)',
@body=@BodyText
-- jonathan.washam@ipcanswers.com

