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
set @TotalRecordsInUnitTestBefore = ( select COUNT(*) from xanalysisofbenchmarks_padportbenchasof )

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
set @TotalCountOfFiles = ( select COUNT(*) from #FileList where Line like 'procoutput PrepBatchTransfer_PerformancePadPortBenchAsOf%' )
if @TotalCountOfFiles <=0
begin
	print 'No files found'
	--GOTO exit_no_files
end


declare @SourceFileFullPath varchar(300)
declare @TotalRecordsAdded int
set @TotalRecordsAdded = 0

DECLARE @SourceFile varchar(100)

if object_id('tempdb..#MoveFilesToArchive') is not null drop table tempdb..#MoveFilesToArchive

CREATE TABLE #MoveFilesToArchive ( Line VARCHAR(512) ) 
-- select * from xanalysisofbenchmarks_padportbenchasof
-- select * from deletethis_xanalysisofbenchmarks_padportbenchasof
--if object_id('deletethis_xanalysisofbenchmarks_padportbenchasof') is not null 
--	drop table deletethis_xanalysisofbenchmarks_padportbenchasof

---- -------------------
---- create deletethis tables
--select * into deletethis_xanalysisofbenchmarks_padportbenchasof from dbo.xanalysisofbenchmarks_padportbenchasof where  1 = 0
----select * into deletethis_GWP_Contact_Addresses from dbo.GWP_Contact_Addresses where  1 = 0
	
--ALTER TABLE deletethis_xanalysisofbenchmarks_padportbenchasof 
--DROP COLUMN SourceFileName

--ALTER TABLE deletethis_xanalysisofbenchmarks_padportbenchasof 
--DROP COLUMN Last_Update
					-- ----------------------------------------
					--SELECT * FROM   #FileList ORDER BY Line ASC
					-- ----------------------------------------

set @notes2 = @notes2 + '
	got here'
-- procoutput PrepBatchTransfer_PerformanceStatic 2016-10-11 114308.csv
--DECLARE @FileRootName varchar(50)
DECLARE @getSourceFile CURSOR
SET @getSourceFile = CURSOR FOR
	SELECT * FROM   #FileList  where Line like 'procoutput PrepBatchTransfer_PerformancePadPortBenchAsOf%' ORDER BY Line asc
OPEN @getSourceFile
	FETCH NEXT
	FROM @getSourceFile INTO @SourceFile
	WHILE @@FETCH_STATUS = 0
	BEGIN
		PRINT @SourceFile

		set @SourceFileFullPath = @PathToFiles + @SourceFile 
		set @notes2 = @notes2 + '
		processing ' + @SourceFileFullPath
-- truncate table xanalysisofbenchmarks_padportbenchasof
		set nocount on
		set nocount OFF
		--set @FileRootName = ''
		----IF left(@SourceFile,len('performancestatic')) = 'performancestatic'
		--set @FileRootName = 'performancestatic'
			
		DECLARE @TableRootName varchar(100)
		set @TableRootName = 'xanalysisofbenchmarks_padportbenchasof'
		
--		IF left(@SourceFile,len('IPC_Contact_Addresses')) = 'IPC_Contact_Addresses'
--			set @FileRootName = 'IPC_Contact_Addresses'
		IF len(@TableRootName) > 0
		BEGIN

			truncate table xanalysisofbenchmarks_padportbenchasof
			--select * from deletethis_xanalysisofbenchmarks_padportbenchasof
			declare @sql varchar(max)

			set @sql = '
			BULK INSERT dbo.xanalysisofbenchmarks_padportbenchasof
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
			

			--SET @sql = ' ' +
			--	'truncate table xanalysisofbenchmarks_padportbenchasof' +
			--	' '
				
			--PRINT '-----'
			--PRINT @sql
			--execute(@sql)
			--PRINT '----- rowcount DELETED'
			--Print @@ROWCOUNT
						
			--SET @sql = ' ' +
			--' INSERT INTO xanalysisofbenchmarks_padportbenchasof ( 
			--	TemplateQuery, AsOfDate, PortfolioList, Portcode, auv_flavour_ext, auv_flavour_name, benchmark_end_weight, class_inception_date, class_last_invested_date, class_node_sec_ext, class_node_sec_id, class_node_sec_name, class_scheme_ext, class_scheme_id, class_scheme_name, ondate_port, port_end_weight, port_mv, portfolio_ext, portfolio_id, portfolio_name, market_index_id, market_index_ext, market_index_name, market_component_id, market_node_sec_id, market_node_sec_ext, market_node_sec_name, market_node_inception_date, market_node_last_invested_date, depth, M1_benchmark, M1_diff, M1_port, M2_benchmark, M2_diff, M2_port, M3_benchmark, M3_diff, M3_port, M4_benchmark, M4_diff, M4_port, M5_benchmark, M5_diff, M5_port, M6_benchmark, M6_diff, M6_port, M7_benchmark, M7_diff, M7_port, M8_benchmark, M8_diff, M8_port, M9_benchmark, M9_diff, M9_port, M10_benchmark, M10_diff, M10_port, M11_benchmark, M11_diff, M11_port, MTD_benchmark, MTD_diff, MTD_port, QTD_benchmark, QTD_diff, QTD_port, sequence, SI_benchmark, SI_diff, SI_port, WTD_port, xml_seq, Y1_benchmark, Y1_diff, Y1_port, Y2_benchmark, Y2_diff, Y2_port, Y3_benchmark, Y3_diff, Y3_port, Y4_benchmark, Y4_diff, Y4_port, Y5_benchmark, Y5_diff, Y5_port, Y6_benchmark, Y6_diff, Y6_port, Y7_benchmark, Y7_diff, Y7_port, Y8_benchmark, Y8_diff, Y8_port, Y9_benchmark, Y9_diff, Y9_port, Y10_benchmark, Y10_diff, Y10_port, Y15_benchmark, Y15_diff, Y15_port, YTD_benchmark, YTD_diff, YTD_port, XmlParameters, XmlOutput, InitDatetime
			--	,Last_Update,SourceFileName ) ' +
			--' SELECT 
			--	TemplateQuery, AsOfDate, PortfolioList, Portcode, auv_flavour_ext, auv_flavour_name, benchmark_end_weight, class_inception_date, class_last_invested_date, class_node_sec_ext, class_node_sec_id, class_node_sec_name, class_scheme_ext, class_scheme_id, class_scheme_name, ondate_port, port_end_weight, port_mv, portfolio_ext, portfolio_id, portfolio_name, market_index_id, market_index_ext, market_index_name, market_component_id, market_node_sec_id, market_node_sec_ext, market_node_sec_name, market_node_inception_date, market_node_last_invested_date, depth, M1_benchmark, M1_diff, M1_port, M2_benchmark, M2_diff, M2_port, M3_benchmark, M3_diff, M3_port, M4_benchmark, M4_diff, M4_port, M5_benchmark, M5_diff, M5_port, M6_benchmark, M6_diff, M6_port, M7_benchmark, M7_diff, M7_port, M8_benchmark, M8_diff, M8_port, M9_benchmark, M9_diff, M9_port, M10_benchmark, M10_diff, M10_port, M11_benchmark, M11_diff, M11_port, MTD_benchmark, MTD_diff, MTD_port, QTD_benchmark, QTD_diff, QTD_port, sequence, SI_benchmark, SI_diff, SI_port, WTD_port, xml_seq, Y1_benchmark, Y1_diff, Y1_port, Y2_benchmark, Y2_diff, Y2_port, Y3_benchmark, Y3_diff, Y3_port, Y4_benchmark, Y4_diff, Y4_port, Y5_benchmark, Y5_diff, Y5_port, Y6_benchmark, Y6_diff, Y6_port, Y7_benchmark, Y7_diff, Y7_port, Y8_benchmark, Y8_diff, Y8_port, Y9_benchmark, Y9_diff, Y9_port, Y10_benchmark, Y10_diff, Y10_port, Y15_benchmark, Y15_diff, Y15_port, YTD_benchmark, YTD_diff, YTD_port, XmlParameters, XmlOutput, InitDatetime
			--	, ''' + convert(varchar,@InitDatetime) + '''' + ', ''' + @SourceFile + '''' +
			--' FROM deletethis_xanalysisofbenchmarks_padportbenchasof '
			
			--PRINT '-----'
			--PRINT @sql
			--execute(@sql)
			PRINT 'rowcount ADDED:'
			PRINT @@ROWCOUNT
			
			
			--// Uncomment this when ready to deply			
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

--delete from xanalysisofbenchmarks_padportbenchasof
--where RowNumber is null

--if object_id('deletethis_xanalysisofbenchmarks_padportbenchasof') is NOT null 
--	DROP TABLE deletethis_xanalysisofbenchmarks_padportbenchasof 

DROP TABLE #FileList

exit_no_files:

DECLARE @rc_xanalysisofbenchmarks_padportbenchasof int
SET @rc_xanalysisofbenchmarks_padportbenchasof = ( SELECT count(*) FROM xanalysisofbenchmarks_padportbenchasof )
--DECLARE @rc_GWP_Contact_Addresses int
--SET @rc_GWP_Contact_Addresses = ( SELECT count(*) FROM GWP_Contact_Addresses )
PRINT 'Process completed successfully'
PRINT '@rc_xanalysisofbenchmarks_padportbenchasof='+converT(varchar,@rc_xanalysisofbenchmarks_padportbenchasof)

--PRINT '@rc_GWP_Contact_Addresses='+converT(varchar,@rc_GWP_Contact_Addresses)
DECLARE @recordsloaded int
set @recordsloaded = ( SELECT count(*) from xanalysisofbenchmarks_padportbenchasof )

set @notes2 = @notes2 + '
Finish Datetime : ' + convert(varchar,getdate())

select convert(varchar,@recordsloaded) + ' records loaded to xanalysisofbenchmarks_padportbenchasof ' + @notes2 
print '.......printing notes.......'
print @notes2
/*

	select * from deletethis_xanalysisofbenchmarks_padportbenchasof
	select * from xanalysisofbenchmarks_padportbenchasof where Portcode = 'SASMO1'
	
	select distinct ManagerStrategy from xanalysisofbenchmarks_padportbenchasof
*/
