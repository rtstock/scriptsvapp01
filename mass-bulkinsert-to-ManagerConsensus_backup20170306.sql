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
set @TotalRecordsInUnitTestBefore = (select COUNT(*) from managerconsensus_imported )

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
set @TotalCountOfFiles = ( select COUNT(*) from #FileList where Line like 'procoutput PrepBatchTransfer_ManagerConsensus%' )

if @TotalCountOfFiles <=0
begin
	print 'No files found'
	GOTO exit_no_files
end
truncate table managerconsensus_imported


declare @SourceFileFullPath varchar(300)
declare @TotalRecordsAdded int
set @TotalRecordsAdded = 0

DECLARE @SourceFile varchar(100)

if object_id('tempdb..#MoveFilesToArchive') is not null drop table tempdb..#MoveFilesToArchive



CREATE TABLE #MoveFilesToArchive ( Line VARCHAR(512) ) 
-- select * from managerconsensus_imported
-- select * from deletethis_GWP_Contact_Addresses
if object_id('deletethis_managerconsensus_imported') is not null 
	drop table deletethis_managerconsensus_imported

-- -------------------
-- create deletethis tables
select * into deletethis_managerconsensus_imported from dbo.managerconsensus_imported where  1 = 0
--select * into deletethis_GWP_Contact_Addresses from dbo.GWP_Contact_Addresses where  1 = 0
	
ALTER TABLE deletethis_managerconsensus_imported 
DROP COLUMN SourceFileName

ALTER TABLE deletethis_managerconsensus_imported 
DROP COLUMN Last_Update
					-- ----------------------------------------
					--SELECT * FROM   #FileList ORDER BY Line ASC
					-- ----------------------------------------

set @notes2 = @notes2 + '
	got here'

DECLARE @FileRootName varchar(50)
DECLARE @getSourceFile CURSOR
SET @getSourceFile = CURSOR FOR
	SELECT * FROM   #FileList  where Line like 'procoutput PrepBatchTransfer_ManagerConsensus%' ORDER BY Line asc
OPEN @getSourceFile
	FETCH NEXT
	FROM @getSourceFile INTO @SourceFile
	WHILE @@FETCH_STATUS = 0
	BEGIN
		PRINT @SourceFile

		set @SourceFileFullPath = @PathToFiles + @SourceFile 
		set @notes2 = @notes2 + '
		processing ' + @SourceFileFullPath
-- truncate table managerconsensus_imported
		set nocount on
		set nocount OFF
		set @FileRootName = ''
		--IF left(@SourceFile,len('ManagerConsensus')) = 'ManagerConsensus'
		set @FileRootName = 'ManagerConsensus'
			
		DECLARE @TableRootName varchar(100)
		set @TableRootName = 'managerconsensus_imported'
		
--		IF left(@SourceFile,len('IPC_Contact_Addresses')) = 'IPC_Contact_Addresses'
--			set @FileRootName = 'IPC_Contact_Addresses'
		IF len(@FileRootName) > 0
		BEGIN

			truncate table deletethis_managerconsensus_imported

			declare @sql varchar(max)

			set @sql = '
			BULK INSERT dbo.deletethis_managerconsensus_imported
			FROM ''' + @SourceFileFullPath + '''
			WITH
			(
			FIRSTROW = 1,
			FIELDTERMINATOR= '','',
			ROWTERMINATOR = ''\n''
			)
			'
			PRINT '---- got here 01 ------'
			PRINT @sql
			execute(@sql)
			
			--select COUNT(*) from deletethis_managerconsensus_imported
			
			SET @sql = ' ' +
				' delete managerconsensus_imported ' +
				' from managerconsensus_imported A ' +
				' , deletethis_managerconsensus_imported B ' +
				' where A.RowNumber = B.RowNumber ' +
				' and A.AsOfDate = B.AsOfDate ' + 
				' '
			PRINT '-----'
			PRINT @sql
			execute(@sql)
			PRINT '----- rowcount DELETED'
			Print @@ROWCOUNT
						
			SET @sql = ' ' +
			' INSERT INTO managerconsensus_imported (RowNumber,ManagerStrategy,PortfolioCode,Manager,Strategy,PositionId,SecurityEffectiveDate,SecurityID,Ticker,IssuerName,SecurityTypeDescription,PortfolioTotalCashMarketValue,PortfolioTotalEquityMarketValue,PortfolioTotalMarketValue,PortfolioSecurityQuantity,SecurityPrice,PortfolioSecurityMarketValue,SecurityPercent_of_PortfolioMarketValue,SecurityPercent_of_PortfolioSecurityMarketValue,SecurityPercent_of_PortfolioEquityMarketValue,CountManagerStrategyPerTicker,ManagerStrategyTotalMarketValue,ManagerStrategySecurityMarketValue,SecurityPercent_of_ManagerStrategyMarketValue,ManagerStrategyTickerCountOfPortfolios,ManagerStrategyCountOfPortfolios,ManagerStrategySecurityModelTargetPercent,SecurityPercentOfManagerStrategyTotalMarketValue,CountAccounts, AsOfDate, HoldYears,Gain_Base,ShortTerm,LongTerm,PortfolioName
				,Last_Update,SourceFileName ) ' +
			' SELECT *, ''' + convert(varchar,@InitDatetime) + '''' + ', ''' + @SourceFile + '''' +
			' FROM deletethis_managerconsensus_imported '
			
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

delete from managerconsensus_imported
where RowNumber is null

if object_id('deletethis_managerconsensus_imported') is NOT null 
	DROP TABLE deletethis_managerconsensus_imported 

DROP TABLE #FileList

exit_no_files:

DECLARE @rc_managerconsensus_imported int
SET @rc_managerconsensus_imported = ( SELECT count(*) FROM managerconsensus_imported )
--DECLARE @rc_GWP_Contact_Addresses int
--SET @rc_GWP_Contact_Addresses = ( SELECT count(*) FROM GWP_Contact_Addresses )
PRINT 'Process completed successfully'
PRINT '@rc_managerconsensus_imported='+converT(varchar,@rc_managerconsensus_imported)

--PRINT '@rc_GWP_Contact_Addresses='+converT(varchar,@rc_GWP_Contact_Addresses)
DECLARE @recordsloaded int
set @recordsloaded = (SELECT count(*) from managerconsensus_imported)

set @notes2 = @notes2 + '
Finish Datetime : ' + convert(varchar,getdate())

select convert(varchar,@recordsloaded) + ' records loaded to managerconsensus_imported ' + @notes2 
print '.......printing notes.......'
print @notes2
/*

	select * from deletethis_managerconsensus_imported
	select * from managerconsensus_imported order by convert(int,RowNumber) asc
	
	select distinct ManagerStrategy from managerconsensus_imported
*/
