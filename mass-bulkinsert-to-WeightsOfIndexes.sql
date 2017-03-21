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

declare @TotalRecordsInUnitTestBefore int
set @TotalRecordsInUnitTestBefore = (select COUNT(*) from weightsofindexes_imported )

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

DECLARE @TotalCountOfFiles int
set @TotalCountOfFiles = ( select COUNT(*) from #FileList where Line like 'sp500weightsassymbols upload%' )
if @TotalCountOfFiles <=0
begin
	print 'No files found'
	--GOTO exit_no_files
end

print '@TotalCountOfFiles = ' + convert(varchar, @TotalCountOfFiles)

--truncate table weightsofindexes_imported


declare @SourceFileFullPath varchar(300)
declare @TotalRecordsAdded int
set @TotalRecordsAdded = 0

DECLARE @SourceFile varchar(100)

if object_id('tempdb..#MoveFilesToArchive') is not null drop table tempdb..#MoveFilesToArchive



CREATE TABLE #MoveFilesToArchive ( Line VARCHAR(512) ) 
-- select * from weightsofindexes_imported
-- select * from deletethis_GWP_Contact_Addresses
if object_id('deletethis_weightsofindexes_imported') is not null 
	drop table deletethis_weightsofindexes_imported

-- -------------------
-- create deletethis tables
select * into deletethis_weightsofindexes_imported from dbo.weightsofindexes_imported where  1 = 0
--select * into deletethis_GWP_Contact_Addresses from dbo.GWP_Contact_Addresses where  1 = 0

ALTER TABLE deletethis_weightsofindexes_imported 
DROP COLUMN [Measure] 

ALTER TABLE deletethis_weightsofindexes_imported 
DROP COLUMN [SourceName] 
	
ALTER TABLE deletethis_weightsofindexes_imported 
DROP COLUMN SourceFileName

ALTER TABLE deletethis_weightsofindexes_imported 
DROP COLUMN Last_Update
					-- ----------------------------------------
					--SELECT * FROM   #FileList ORDER BY Line ASC
					-- ----------------------------------------

set @notes2 = @notes2 + '
	got here'

DECLARE @FileRootName varchar(50)
DECLARE @getSourceFile CURSOR
SET @getSourceFile = CURSOR FOR
	SELECT * FROM   #FileList  where Line like 'sp500weightsassymbols upload%' ORDER BY Line asc
OPEN @getSourceFile
	FETCH NEXT
	FROM @getSourceFile INTO @SourceFile
	WHILE @@FETCH_STATUS = 0
	BEGIN
		PRINT '@SourceFile = ' + @SourceFile

		set @SourceFileFullPath = @PathToFiles + @SourceFile 
		set @notes2 = @notes2 + '
		processing ' + @SourceFileFullPath
-- truncate table weightsofindexes_imported
		set nocount on
		set nocount OFF
		set @FileRootName = ''
		--IF left(@SourceFile,len('sp500weights')) = 'sp500weights'
		set @FileRootName = 'sp500weights'
			
		DECLARE @TableRootName varchar(100)
		set @TableRootName = 'weightsofindexes_imported'
		print 'ok got here 10'
		
--		IF left(@SourceFile,len('IPC_Contact_Addresses')) = 'IPC_Contact_Addresses'
--			set @FileRootName = 'IPC_Contact_Addresses'
		IF len(@TableRootName) > 0
		BEGIN
			print 'ok got here 20'
			truncate table deletethis_weightsofindexes_imported

			declare @sql varchar(max)

			set @sql = '
			BULK INSERT dbo.deletethis_weightsofindexes_imported
			FROM ''' + @SourceFileFullPath + '''
			WITH
			(
			FIRSTROW = 2,
			FIELDTERMINATOR= '','',
			ROWTERMINATOR = ''\n''
			)
			'
			PRINT '---- got here 01 ------'
			PRINT @sql
			execute(@sql)
			
			update deletethis_weightsofindexes_imported
			set Period = REPLACE(Period,'"','')
			
			update deletethis_weightsofindexes_imported
			set Ticker = REPLACE(Ticker,'"','')
						
			select COUNT(*) from deletethis_weightsofindexes_imported
			
			SET @sql = ' ' +
				' delete weightsofindexes_imported ' +
				' from weightsofindexes_imported A ' +
				' where A.SourceFileName = ''' + @SourceFile + '''' +
				' '
			PRINT '-----'
			PRINT @sql
			execute(@sql)
			PRINT '----- rowcount DELETED'
			Print @@ROWCOUNT
			
			
			/*
			drop table weightsofindexes_imported
			create table weightsofindexes_imported (rowid varchar(7),a_symbol varchar(30) ,b_monthend varchar(15) ,d_end varchar(20) ,e_pctchange varchar(50),Last_Update datetime,SourceFileName varchar(200))
			*/
			
			SET @sql = ' ' +
			' INSERT INTO weightsofindexes_imported ( Period, Ticker, DataValue, Measure, SourceName, Last_Update, SourceFileName ) ' +
			' SELECT *, ''Weight'', ''SP500'', ''' + convert(varchar,@InitDatetime) + '''' + ', ''' + @SourceFile + '''' +
			' FROM deletethis_weightsofindexes_imported '
			
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

--delete from weightsofindexes_imported
--where RowID is null

if object_id('deletethis_weightsofindexes_imported') is NOT null 
	DROP TABLE deletethis_weightsofindexes_imported 

DROP TABLE #FileList

exit_no_files:

DECLARE @rc_weightsofindexes_imported int
SET @rc_weightsofindexes_imported = ( SELECT count(*) FROM weightsofindexes_imported )
--DECLARE @rc_GWP_Contact_Addresses int
--SET @rc_GWP_Contact_Addresses = ( SELECT count(*) FROM GWP_Contact_Addresses )
PRINT 'Process completed successfully'
PRINT '@rc_weightsofindexes_imported='+converT(varchar,@rc_weightsofindexes_imported)

--delete from weightsofindexes_imported
--where rowid = 'rowid'

--PRINT '@rc_GWP_Contact_Addresses='+converT(varchar,@rc_GWP_Contact_Addresses)
DECLARE @recordsloaded int
set @recordsloaded = (SELECT count(*) from weightsofindexes_imported)

set @notes2 = @notes2 + '
Finish Datetime : ' + convert(varchar,getdate())

select convert(varchar,@recordsloaded) + ' records loaded to weightsofindexes_imported ' + @notes2 
print '.......printing notes.......'
print @notes2
/*

	select * from deletethis_weightsofindexes_imported
	select * from weightsofindexes_imported 
	
	select distinct ManagerStrategy from weightsofindexes_imported
*/
