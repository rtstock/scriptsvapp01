use DataAgg


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
set @TotalRecordsInUnitTestBefore = (select COUNT(*) from aumoriginalcodeblock_imported )

--truncate table aumoriginalcodeblock_imported

declare @PathToFiles varchar(300)
set @PathToFiles = '\\ipc-vsql01\Data\Batches\prod\AUMOriginalCodeBlock\incoming\'

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
-- select * from aumoriginalcodeblock_imported
-- select * from deletethis_GWP_Contact_Addresses
if object_id('deletethis_aumoriginalcodeblock_imported') is not null 
	drop table deletethis_aumoriginalcodeblock_imported

-- -------------------
-- create deletethis tables
select * into deletethis_aumoriginalcodeblock_imported from dbo.aumoriginalcodeblock_imported where  1 = 0
--select * into deletethis_GWP_Contact_Addresses from dbo.GWP_Contact_Addresses where  1 = 0
	
ALTER TABLE deletethis_aumoriginalcodeblock_imported 
DROP COLUMN SourceFileName

ALTER TABLE deletethis_aumoriginalcodeblock_imported 
DROP COLUMN Last_Update
					-- ----------------------------------------
					--SELECT * FROM   #FileList ORDER BY Line ASC
					-- ----------------------------------------

--truncate table aumoriginalcodeblock_imported
set @notes2 = @notes2 + '
	got here'

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
-- truncate table aumoriginalcodeblock_imported
		set nocount on
		set nocount OFF
		set @FileRootName = ''
		IF left(@SourceFile,len('AUMOriginalCodeBlock')) = 'AUMOriginalCodeBlock'
			set @FileRootName = 'AUMOriginalCodeBlock'
			
		DECLARE @TableRootName varchar(100)
		set @TableRootName = 'aumoriginalcodeblock_imported'
		
--		IF left(@SourceFile,len('IPC_Contact_Addresses')) = 'IPC_Contact_Addresses'
--			set @FileRootName = 'IPC_Contact_Addresses'
		IF len(@FileRootName) > 0
		BEGIN

			truncate table deletethis_aumoriginalcodeblock_imported

			declare @sql varchar(max)

			set @sql = '
			BULK INSERT dbo.deletethis_aumoriginalcodeblock_imported
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
			
			--select COUNT(*) from deletethis_aumoriginalcodeblock_imported
			
			SET @sql = ' ' +
				' delete aumoriginalcodeblock_imported ' +
				' from aumoriginalcodeblock_imported A ' +
				' , deletethis_aumoriginalcodeblock_imported B ' +
				' where A.AsOfDate = B.AsOfDate ' +
				' ' + 
				' '
			PRINT '-----'
			PRINT @sql
			execute(@sql)
			PRINT '----- rowcount DELETED'
			Print @@ROWCOUNT
						
			SET @sql = ' ' +
			' INSERT INTO aumoriginalcodeblock_imported (
					MasterPortfolioStateCode, PortfolioRoot, MoneyManager, client_subgroup, PortfolioId, PortfolioCode, PortfolioName, CustodianName, TerritoryCode, CustodianAccountNumber, TaxFilingNumber, InitializationDate, ManagementDate, PortfolioEffectiveDate, IsMasterFlag, IsActiveFlag, TaxSensitivityCode, InvestmentObjectiveDescription, InvestmentDiscretionDescription, AccountTypeDescription, RRDescription, PortfolioStateCode, InvesmentEntityCode, AccountSubTypeCode, AccountSubTypeDescription, TerminationDate, investment_objective_code, ValueOfSecurities, ValueOfAccruedIncome, ValueOfCash, AcctType, correspondence_firm, correspondence_firm_name, TotalMarketValue, AsOfDate, Last_Update, SourceFileName
				) ' +
			' SELECT *, ''' + convert(varchar,@InitDatetime) + '''' + ', ''' + @SourceFile + '''' +
			' FROM deletethis_aumoriginalcodeblock_imported '
			
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

delete from aumoriginalcodeblock_imported
where PortfolioCode is null

update aumoriginalcodeblock_imported
set PortfolioName = replace(PortfolioName,'"','')

if object_id('deletethis_aumoriginalcodeblock_imported') is NOT null 
	DROP TABLE deletethis_aumoriginalcodeblock_imported 

DROP TABLE #FileList



DECLARE @rc_aumoriginalcodeblock_imported int
SET @rc_aumoriginalcodeblock_imported = ( SELECT count(*) FROM aumoriginalcodeblock_imported )
--DECLARE @rc_GWP_Contact_Addresses int
--SET @rc_GWP_Contact_Addresses = ( SELECT count(*) FROM GWP_Contact_Addresses )
PRINT 'Process completed successfully'
PRINT '@rc_aumoriginalcodeblock_imported='+converT(varchar,@rc_aumoriginalcodeblock_imported)

--PRINT '@rc_GWP_Contact_Addresses='+converT(varchar,@rc_GWP_Contact_Addresses)
DECLARE @recordsloaded int
set @recordsloaded = (SELECT count(*) from aumoriginalcodeblock_imported)

set @notes2 = @notes2 + '
Finish Datetime : ' + convert(varchar,getdate())

select convert(varchar,@recordsloaded) + ' records loaded to aumoriginalcodeblock_imported ' + @notes2 
/*

select * from deletethis_aumoriginalcodeblock_imported
select * from aumoriginalcodeblock_imported order by convert(int,RowNumber) asc

*/
