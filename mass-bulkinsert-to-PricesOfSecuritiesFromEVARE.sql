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
drop table PricesOfSecuritiesFromEVARE_imported
create table PricesOfSecuritiesFromEVARE_imported (CUSIP varchar(20),	TICKER varchar(20),	Security_Name varchar(100),	Short_Description varchar(200),	GWP_Price	varchar(20), AsOfDate varchar(20), Last_Update datetime, SourceFileName varchar(200))
*/
declare @TotalRecordsInUnitTestBefore int
set @TotalRecordsInUnitTestBefore = (select COUNT(*) from PricesOfSecuritiesFromEVARE_imported )

--truncate table PricesOfSecuritiesFromEVARE_imported

declare @PathToFiles varchar(300)
set @PathToFiles = '\\ipc-vsql01\Data\Batches\prod\WatchFolder\incoming\'

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
-- select * from PricesOfSecuritiesFromEVARE_imported
-- select * from deletethis_GWP_Contact_Addresses
if object_id('deletethis_PricesOfSecuritiesFromEVARE_imported') is not null 
	drop table deletethis_PricesOfSecuritiesFromEVARE_imported

-- -------------------
-- create deletethis tables
select * into deletethis_PricesOfSecuritiesFromEVARE_imported from dbo.PricesOfSecuritiesFromEVARE_imported where  1 = 0
--select * into deletethis_GWP_Contact_Addresses from dbo.GWP_Contact_Addresses where  1 = 0
	
--ALTER TABLE deletethis_PricesOfSecuritiesFromEVARE_imported 
--DROP COLUMN SourceFileName

--ALTER TABLE deletethis_PricesOfSecuritiesFromEVARE_imported 
--DROP COLUMN Last_Update
					-- ----------------------------------------
					--SELECT * FROM   #FileList ORDER BY Line ASC
					-- ----------------------------------------

--truncate table PricesOfSecuritiesFromEVARE_imported
set @notes2 = @notes2 + '
	got here'

declare @CountFilesProcessed int
set @CountFilesProcessed  = 0

truncate table deletethis_PricesOfSecuritiesFromEVARE_imported

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
-- truncate table PricesOfSecuritiesFromEVARE_imported
		set nocount on
		--set nocount OFF
		set @FileRootName = ''
		IF left(@SourceFile,len('ComericaIPCPrice')) = 'ComericaIPCPrice'
			set @FileRootName = 'ComericaIPCPrice'
		IF left(@SourceFile,len('PershingIPCPrice')) = 'PershingIPCPrice'
			set @FileRootName = 'PershingIPCPrice'
		IF left(@SourceFile,len('WFAIPCPrice')) = 'WFAIPCPrice'
			set @FileRootName = 'WFAIPCPrice'

			
		DECLARE @TableRootName varchar(100)
		set @TableRootName = 'PricesOfSecuritiesFromEVARE_imported'
		
--		IF left(@SourceFile,len('IPC_Contact_Addresses')) = 'IPC_Contact_Addresses'
--			set @FileRootName = 'IPC_Contact_Addresses'
		IF len(@FileRootName) > 0
		BEGIN
			set @CountFilesProcessed  = @CountFilesProcessed  + 1
			
			declare @sql varchar(max)

			set @sql = '
			truncate table CsvDataReader
			BULK INSERT dbo.CsvDataReader
			FROM ''' + @SourceFileFullPath + '''
			WITH
			(
			FIRSTROW = 2,
			FIELDTERMINATOR= ''|'',
			ROWTERMINATOR = ''\n''
			)
			'
			execute(@sql)
			--select Field1, CUSIP, AsOfDate, Field4, Field5, Field6, Field7, DataValue, Field9, Field10, Field11, Field12, Field13,Field14, SourceFileName, Last_Update from deletethis_PricesOfSecuritiesFromEVARE_imported

			insert into deletethis_PricesOfSecuritiesFromEVARE_imported (Field1, CUSIP, AsOfDate, Field4, Field5, Field6, Field7, Field8, DataValue, Field10, Field11, Field12, Field13, Field14, SourceFileName, Last_Update)
			select 
				dbo.SPLIT(DataRow,',',1) Field1
				, dbo.SPLIT(DataRow,',',2) CUSIP
				, dbo.SPLIT(DataRow,',',3) AsOfDate
				, dbo.SPLIT(DataRow,',',4) Field4
				, dbo.SPLIT(DataRow,',',5) Field5
				, dbo.SPLIT(DataRow,',',6) Field6
				, dbo.SPLIT(DataRow,',',7) Field7
				, dbo.SPLIT(DataRow,',',8) Field8
				, dbo.SPLIT(DataRow,',',9) DataValue
				, dbo.SPLIT(DataRow,',',10) Field10
				, dbo.SPLIT(DataRow,',',11) Field11
				, dbo.SPLIT(DataRow,',',12) Field12
				, dbo.SPLIT(DataRow,',',13)  Field13
				, dbo.SPLIT(DataRow,',',14)  Field14
				, @SourceFile SourceFileName
				, GETDATE()  Last_Update
			from CsvDataReader
			--select * from CsvDataReader
			--PRINT '---- got here 01 ------'
			--PRINT @sql
			--execute(@sql)
			
			--SET @sql = ' ' +
			--	' delete PricesOfSecuritiesFromEVARE_imported ' +
			--	' from PricesOfSecuritiesFromEVARE_imported A ' +
			--	' , deletethis_PricesOfSecuritiesFromEVARE_imported B ' +
			--	' where A.AsOfDate = B.AsOfDate ' +
			--	' ' + 
			--	' '
			--PRINT '-----'
			--PRINT @sql
			--execute(@sql)
			--PRINT '----- rowcount DELETED'
			--Print @@ROWCOUNT
						
			--SET @sql = ' ' +
			--' INSERT INTO PricesOfSecuritiesFromEVARE_imported (
			--		CUSIP, TICKER, Security_Name, Short_Description, GWP_Price, AsOfDate, Last_Update, SourceFileName
			--	) ' +
			--' SELECT *, ''' + convert(varchar,@InitDatetime) + '''' + ', ''' + @SourceFile + '''' +
			--' FROM deletethis_PricesOfSecuritiesFromEVARE_imported '
			
			--PRINT '-----'
			--PRINT @sql
			--execute(@sql)
			--PRINT 'rowcount:'
			--PRINT @@ROWCOUNT
			
			
			----SET @sql = ' DELETE '+ @TableRootName +
			----' WHERE Last_Name = ' + '''Last Name'''
			----PRINT @sql
			----execute(@sql)
			
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

--select * from deletethis_PricesOfSecuritiesFromEVARE_imported

delete PricesOfSecuritiesFromEVARE_imported
from PricesOfSecuritiesFromEVARE_imported A, deletethis_PricesOfSecuritiesFromEVARE_imported B
where CONVERT(datetime, A.AsOfDate) = CONVERT(datetime, B.AsOfDate) 
and left(A.SourceFileName,10) = left(B.SourceFileName,10)

print 'count of rows deleted from PricesOfSecuritiesFromEVARE_imported= ' + convert(varchar(10), @@ROWCOUNT)

insert into PricesOfSecuritiesFromEVARE_imported
select * from deletethis_PricesOfSecuritiesFromEVARE_imported
--if object_id('deletethis_PricesOfSecuritiesFromEVARE_imported') is NOT null 
--	DROP TABLE deletethis_PricesOfSecuritiesFromEVARE_imported 

DROP TABLE #FileList



DECLARE @rc_PricesOfSecuritiesFromEVARE_imported int
SET @rc_PricesOfSecuritiesFromEVARE_imported = ( SELECT count(*) FROM PricesOfSecuritiesFromEVARE_imported )
--DECLARE @rc_GWP_Contact_Addresses int
--SET @rc_GWP_Contact_Addresses = ( SELECT count(*) FROM GWP_Contact_Addresses )
PRINT 'Process completed successfully'
PRINT '@rc_PricesOfSecuritiesFromEVARE_imported='+converT(varchar,@rc_PricesOfSecuritiesFromEVARE_imported)

--PRINT '@rc_GWP_Contact_Addresses='+converT(varchar,@rc_GWP_Contact_Addresses)

DECLARE @TotalRecordsInUnitTestAfter int
set @TotalRecordsInUnitTestAfter = (SELECT count(*) from PricesOfSecuritiesFromEVARE_imported)

set @notes2 = @notes2 + '
Finish Datetime : ' + convert(varchar,getdate())

select convert(varchar, @CountFilesProcessed) + ' files processed.  ' + convert(varchar,@TotalRecordsInUnitTestBefore) + ' records before, ' + convert(varchar,@TotalRecordsInUnitTestAfter) + ' records after.  ' + @notes2 

/*

select * from deletethis_PricesOfSecuritiesFromEVARE_imported
select * from PricesOfSecuritiesFromEVARE_imported order by convert(int,RowNumber) asc
truncate table PricesOfSecuritiesFromEVARE_imported
select * from PricesOfSecuritiesFromEVARE_imported

*/
