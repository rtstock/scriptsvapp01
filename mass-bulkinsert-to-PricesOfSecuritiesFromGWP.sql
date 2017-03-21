use DataAgg
set nocount on

/*
	EXEC sp_configure 'show advanced options', 1
	
*/

-- -----------------------------------------------
-- Begin the process to import the AcctMaster file
declare @InitDatetime datetime
set @InitDatetime = GETDATE()

declare @notes2 varchar(max)
set @notes2 = ''

set @notes2 = @notes2 + '
Start Datetime : ' + convert(varchar,getdate())
/*
drop table PricesOfSecuritiesFromGWP_imported
create table PricesOfSecuritiesFromGWP_imported (CUSIP varchar(20),	TICKER varchar(20),	Security_Name varchar(100),	Short_Description varchar(200),	GWP_Price	varchar(20), AsOfDate varchar(20), Last_Update datetime, SourceFileName varchar(200))
*/
declare @TotalRecordsInUnitTestBefore int
set @TotalRecordsInUnitTestBefore = (select COUNT(*) from PricesOfSecuritiesFromGWP_imported )

--truncate table PricesOfSecuritiesFromGWP_imported

declare @PathToFiles varchar(300)
set @PathToFiles = '\\ipc-vsql01\Data\Batches\prod\PricesOfSecuritiesFromGWP\incoming\'

if object_id('tempdb..#FileList') is not null drop table tempdb..#FileList
CREATE TABLE #FileList ( 
  Line VARCHAR(512)) 
DECLARE @Path varchar(256) = 'dir ' + @PathToFiles
DECLARE @Command varchar(1024) =  @Path+' /A-D  /B' 
PRINT @Command
		set @notes2 = @notes2 + '
		Command ' + @Command
INSERT #FileList 
EXEC MASTER.dbo.xp_cmdshell @Command
DELETE #FileList WHERE  Line IS NULL 
 
declare @SourceFileFullPath varchar(300)
declare @TotalRecordsAdded int
set @TotalRecordsAdded = 0

DECLARE @SourceFile varchar(100)

if object_id('tempdb..#MoveFilesToArchive') is not null drop table tempdb..#MoveFilesToArchive

CREATE TABLE #MoveFilesToArchive ( Line VARCHAR(512) ) 
-- select * from PricesOfSecuritiesFromGWP_imported
-- select * from deletethis_GWP_Contact_Addresses
if object_id('deletethis_PricesOfSecuritiesFromGWP_imported') is not null 
	drop table deletethis_PricesOfSecuritiesFromGWP_imported

-- -------------------
-- create deletethis tables
select * into deletethis_PricesOfSecuritiesFromGWP_imported from dbo.PricesOfSecuritiesFromGWP_imported where  1 = 0
--select * into deletethis_GWP_Contact_Addresses from dbo.GWP_Contact_Addresses where  1 = 0
	
ALTER TABLE deletethis_PricesOfSecuritiesFromGWP_imported 
DROP COLUMN SourceFileName

ALTER TABLE deletethis_PricesOfSecuritiesFromGWP_imported 
DROP COLUMN Last_Update
					-- ----------------------------------------
					--SELECT * FROM   #FileList ORDER BY Line ASC
					-- ----------------------------------------

--truncate table PricesOfSecuritiesFromGWP_imported
set @notes2 = @notes2 + '
	got here'
DECLARE @FileCount int
set @FileCount = 0
DECLARE @FileRootName varchar(50)
DECLARE @getSourceFile CURSOR
SET @getSourceFile = CURSOR FOR
	SELECT * FROM   #FileList ORDER BY Line asc
OPEN @getSourceFile
	FETCH NEXT
	FROM @getSourceFile INTO @SourceFile
	WHILE @@FETCH_STATUS = 0
	BEGIN
		PRINT @SourceFile

		set @SourceFileFullPath = @PathToFiles + @SourceFile 
		set @notes2 = @notes2 + '
		processing ' + @SourceFileFullPath
-- truncate table PricesOfSecuritiesFromGWP_imported
		set nocount on
		set nocount OFF
		set @FileRootName = ''
		IF left(@SourceFile,len('PricesOfSecuritiesFromGWP')) = 'PricesOfSecuritiesFromGWP'
			set @FileRootName = 'PricesOfSecuritiesFromGWP'
			
		DECLARE @TableRootName varchar(100)
		set @TableRootName = 'PricesOfSecuritiesFromGWP_imported'
		
--		IF left(@SourceFile,len('IPC_Contact_Addresses')) = 'IPC_Contact_Addresses'
--			set @FileRootName = 'IPC_Contact_Addresses'
		IF len(@FileRootName) > 0
		BEGIN
			set @FileCount = @FileCount + 1
			truncate table deletethis_PricesOfSecuritiesFromGWP_imported

			declare @sql varchar(max)

			set @sql = '
			BULK INSERT dbo.deletethis_PricesOfSecuritiesFromGWP_imported
			FROM ''' + @SourceFileFullPath + '''
			WITH
			(
			FIRSTROW = 2,
			FIELDTERMINATOR= ''\t'',
			ROWTERMINATOR = ''\n''
			)
			'
			PRINT '---- got here 01 ------'
			PRINT @sql
			execute(@sql)
			
			--select COUNT(*) from deletethis_PricesOfSecuritiesFromGWP_imported
			
			SET @sql = ' ' +
				' delete PricesOfSecuritiesFromGWP_imported ' +
				' from PricesOfSecuritiesFromGWP_imported A ' +
				' , deletethis_PricesOfSecuritiesFromGWP_imported B ' +
				' where A.AsOfDate = B.AsOfDate ' +
				' ' + 
				' '
			PRINT '-----'
			PRINT @sql
			execute(@sql)
			PRINT '----- rowcount DELETED'
			Print @@ROWCOUNT
						
			SET @sql = ' ' +
			' INSERT INTO PricesOfSecuritiesFromGWP_imported (
					CUSIP, TICKER, Security_Name, Short_Description, GWP_Price, AsOfDate, Last_Update, SourceFileName
				) ' +
			' SELECT *, ''' + convert(varchar,@InitDatetime) + '''' + ', ''' + @SourceFile + '''' +
			' FROM deletethis_PricesOfSecuritiesFromGWP_imported '
			
			PRINT '-----'
			PRINT @sql
			execute(@sql)
			PRINT 'rowcount:'
			PRINT @@ROWCOUNT
			
			
			--SET @sql = ' DELETE '+ @TableRootName +
			--' WHERE Last_Name = ' + '''Last Name'''
			--PRINT @sql
			--execute(@sql)
			
			SET @Command = 'move "' + @SourceFileFullPath + '" "'+ @PathToFiles + 'Archive\'+ @SourceFile +'"'
			PRINT '@Command ' + @Command 
			set @notes2 = @notes2 + '
			@Command ' + @Command 
			INSERT #MoveFilesToArchive
			EXEC MASTER.dbo.xp_cmdshell @Command
			DELETE #MoveFilesToArchive WHERE  Line IS NULL 

		END 
	
	FETCH NEXT
	FROM @getSourceFile INTO @SourceFile
	END
CLOSE @getSourceFile
DEALLOCATE @getSourceFile

--delete from PricesOfSecuritiesFromGWP_imported
--where PortfolioCode is null

--update PricesOfSecuritiesFromGWP_imported
--set PortfolioName = replace(PortfolioName,'"','')

if object_id('deletethis_PricesOfSecuritiesFromGWP_imported') is NOT null 
	DROP TABLE deletethis_PricesOfSecuritiesFromGWP_imported 

DROP TABLE #FileList



DECLARE @rc_PricesOfSecuritiesFromGWP_imported int
SET @rc_PricesOfSecuritiesFromGWP_imported = ( SELECT count(*) FROM PricesOfSecuritiesFromGWP_imported )
--DECLARE @rc_GWP_Contact_Addresses int
--SET @rc_GWP_Contact_Addresses = ( SELECT count(*) FROM GWP_Contact_Addresses )
PRINT 'Process completed successfully'
PRINT '@rc_PricesOfSecuritiesFromGWP_imported='+converT(varchar,@rc_PricesOfSecuritiesFromGWP_imported)

--PRINT '@rc_GWP_Contact_Addresses='+converT(varchar,@rc_GWP_Contact_Addresses)
DECLARE @recordsloaded int
set @recordsloaded = (SELECT count(*) from PricesOfSecuritiesFromGWP_imported)

set @notes2 = @notes2 + '
Finish Datetime : ' + convert(varchar,getdate())

-- Run the bat to get the evare files now...
if @FileCount > 0
BEGIN
	SET @Command = 'CALL "' + 'C:\Batches\AutomationProjects\Watcher\code\bat\$execute_ftp_functions-evare.bat' +'"'
	PRINT '@Command ' + @Command 
	set @notes2 = @notes2 + '
	@Command ' + @Command 
	INSERT #MoveFilesToArchive
	EXEC MASTER.dbo.xp_cmdshell @Command
	DELETE #MoveFilesToArchive WHERE  Line IS NULL 
	--C:\Batches\AutomationProjects\Watcher\code\bat\$execute_ftp_functions-evare.bat
END

select convert(varchar,@recordsloaded) + ' records loaded to PricesOfSecuritiesFromGWP_imported ' + @notes2 

/*

select * from deletethis_PricesOfSecuritiesFromGWP_imported
select * from PricesOfSecuritiesFromGWP_imported order by convert(int,RowNumber) asc

*/
