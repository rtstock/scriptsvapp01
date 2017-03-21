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
declare @date_AUMOriginalCodeBlock datetime
declare @asofdate_AUMOriginalCodeBlock varchar(100)



-- select MAX(Last_Update) from DataAgg..GWP_Accounts 

-- select @date_AUMOriginalCodeBlock = MAX(Last_Update) from DataAgg.dbo.aumoriginalcodeblock_imported 
-- select @asofdate_AUMOriginalCodeBlock = MAX(SourceFileName) from DataAgg.dbo.aumoriginalcodeblock_imported 

select @date_AUMOriginalCodeBlock = max(Last_Update), @asofdate_AUMOriginalCodeBlock = MAX(AsOfDate) from DataAgg.dbo.vAUMOriginalCodeBlock 


declare @BodyText varchar(max)
set  @BodyText ='Database table automatic update notice:'
	+ CHAR(10) 
	+ CHAR(10) 
	+ '  ' + char(149) + '  ' + @@SERVERNAME + '.DataAgg.dbo.aumoriginalcodeblock_imported was last updated ' + convert(varchar(23),@date_AUMOriginalCodeBlock) + ' with AsOfDate of ' + @asofdate_AUMOriginalCodeBlock
	+ CHAR(10) 
	+ CHAR(10)
	+'Source: C:\Batches\AutomationProjects\Watcher\code\bat\send_email_of_results_sql_aumoriginalcodeblock_imported.sql'
	+'Notes: This data set is updated using SS&C PAGES email of a csv file to justin.malinchak@ipcanswers.com.  The file is detached, placed onto a folder on ipc-vsql01.  Then a periodic "watcher" process on ipc-vapp01 loads the data to the DataAgg database on ipc-vsql01'
print @BodyText

EXEC sp_send_dbmail @profile_name='IPC Mail Profile',
@recipients='justin.malinchak@ipcanswers.com;',
@subject='Job: AUMOriginalCodeBlock (Daily)',
@body=@BodyText
-- jonathan.washam@ipcanswers.com

