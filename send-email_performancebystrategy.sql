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

-- These are the parameters passed in from Powershell
declare @MyPeriod varchar(25)
set @MyPeriod = '$(Period)' --'MTD'
--set @MyPeriod = 'MTD'

declare @myfilename varchar(200)
set @myfilename = '$(myfilename)' --'M:\Batches\prod\Performance\output\Median_Performance_Full_2017-01-10.xls'
--set @myfilename = '\\IPC-VSQL01\DATA\Batches\prod\Performance\output\Median_Performance_Full_2017-01-10.xls'

print '$$$$$ @MyPeriod $$$$$$'
print @MyPeriod
--SMALL SECTION OF CODE FROM ADDCITY.SQL
--INSERT #CITY VALUES
--	('$(CITY)', '$(STATE)', '$(COUNTRY)');


/*
Exclustions
-----------
John Hancock Asset Mgt (MFC) MFCLC1 - FUNDAMENTAL 
Logan Capital Mgt Inc LCMLG2 - GROWTH
SouthernSun Asset Mgt SSASMI - MACON
The Boston Company Asset Mgt LLC LGAMV3 - MACON
Tributary Capital Mgt TCMSV4  Small Cap Value

*/

-- ==================================================================================================================================
-- This is the DETAIL
-- ==================================================================================================================================
--set nocount on
if object_id('tempdb..#_201612070938_00') is not null drop table tempdb..#_201612070938_00
if object_id('tempdb..#_201612070938_01') is not null drop table tempdb..#_201612070938_01
if object_id('tempdb..#_201612070938_02') is not null drop table tempdb..#_201612070938_02


create table #_201612070938_00
(
	RowID	varchar(50)	,
	ManagerStrategy	varchar(300)	,
	PortcodeList	varchar(50)	,
	portfolio_id	int	,
	portfolio_ext	varchar(50),	
	auv_flavour_ext	varchar(50),	
	class_inception_date	varchar(25)	,
	class_last_invested_date	varchar(25)	,
	ROR	float	,
	last_update	datetime	,
	Period varchar(10),
	TotalMarketValue	float	

) 

insert into #_201612070938_00
exec DataAgg..analysisofbenchmarks_performancemedianstatistics @MyPeriod,null,1
----------------------------------------------------------------------------------------------
select *
into #_201612070938_01
from
(
	select B.prt_Relationship_title, A.*
	from
	(
		SELECT B.CLIENT_SUBGROUP, A.*
		from
		(
			select * from #_201612070938_00
		) A
		left outer join
		(
			select distinct Portfolio_Code,CLIENT_SUBGROUP 
			from GWP_Extract.dbo.AcctMaster
		) B
		on A.PortcodeList = B.Portfolio_Code
	) A
	left outer join
	(
		select CLIENT_SUBGROUP,MIN(prt_Relationship_title) prt_Relationship_title 
		from DataAgg.dbo.v_MPS_Sequence
		group by CLIENT_SUBGROUP
	) B
	on A.CLIENT_SUBGROUP = B.CLIENT_SUBGROUP
) A

-- select * from #_201612070938_01

/*
-- This finds if there are any duplicates in v_MPS_Sequence 

	select *
	from
	(
		select CLIENT_SUBGROUP,count(*) Ct
		from
		(
			select distinct CLIENT_SUBGROUP, prt_Relationship_title
			from 
			(
				select * from DataAgg.dbo.v_MPS_Sequence
			) A
		) A
		group by CLIENT_SUBGROUP
	) A
	where Ct > 1

*/

select *
into #_201612070938_02
from
(
	select A.CLIENT_SUBGROUP RelationshipCode,replace(A.prt_Relationship_title,',','') RelationshipTitle, A.PortcodeList PortfolioCode, B.Custodian_Account_Number CustodianAccountNumber, B.Portfolio_Name PortfolioName,A.ManagerStrategy
	, B.Investment_Entity_Description MoneyManager

	, A.class_inception_date InceptionDate
	, replace(convert(varchar,B.Activity_Start_Date,111),'/','-') ActivityStartDate
	, A.class_last_invested_date AsOfDate

	--, replace(convert(varchar,A.class_inception_date,111),'/','-') InceptionDate
	--, B.Activity_Start_Date ActivityStartDate
	--, replace(convert(varchar,A.class_last_invested_date),'/','-') AsOfDate

	, convert(money,A.TotalMarketValue) TotalMarketValue, B.Base_Total_Cash TotalCash, A.Period PerformancePeriod,A.ROR
	, A.auv_flavour_ext GrossOrNetOfFees
	, B.Account_Type_Description AccountType
	, case when B.Account_Subtype_Description is null then 'SMA' else B.Account_Subtype_Description end Account_Subtype_Description
	, B.Portfolio_Model_Name, B.Investment_Objective_Description, B.Residence_Region, B.RR_Description AdvisorName, B.PORTFOLIO_MANAGER_Description MoneyManagerName, B.CORRESPONDENCE_FIRM_Description CorrespondenceFirmName
	--, A.last_update
	, replace(convert(varchar,A.last_update),'/','-') last_update
	, A.RowID MyStrategyID
	from
	(
		select * from #_201612070938_01
	) A
	left outer join
	(
		select * from GWP_Extract.dbo.AcctMaster

	) B
	on A.PortcodeList = B.Portfolio_Code
) A

-- select * from #_201612070938_02


if OBJECT_ID('DataAgg.dbo.proctable_analysisofbenchmarks_performancebyaccount') is not null drop table DataAgg.dbo.proctable_analysisofbenchmarks_performancebyaccount


select *
into DataAgg.dbo.proctable_analysisofbenchmarks_performancebyaccount
from #_201612070938_02

declare @datestring varchar(25)
set @datestring = (
	select top 1 *
	from
	(
		--select top 1 replace(convert(varchar,AsOfDate,111),'/','') + '-' + replace(convert(varchar,GETDATE(),114),':','') Print_Effective_Date
		select top 1 replace(AsOfDate,'-','') + '-' + replace(convert(varchar,GETDATE(),114),':','') Print_Effective_Date
		from DataAgg.dbo.proctable_analysisofbenchmarks_performancebyaccount
		order by AsOfDate desc
	) A
)
-- select * from DataAgg.dbo.proctable_analysisofbenchmarks_performancebyaccount

print '@datestring:'
print @datestring
/*

*/
declare @outputfilepathname_detail varchar(300)
set @outputfilepathname_detail = '\\ipc-vsql01\DATA\Batches\prod\Performance\output\PerformanceOfAccounts_'+@datestring+'.csv'
print '@outputfilepathname_detail:'
print @outputfilepathname_detail
-- ''RelationshipCode'' RelationshipCode,''RelationshipTitle'' RelationshipTitle,''PortfolioCode'' PortfolioCode,''CustodianAccountNumber'' CustodianAccountNumber,''PortfolioName'' PortfolioName,''ManagerStrategy'' ManagerStrategy,''InceptionDate'' InceptionDate,''ActivityStartDate'' ActivityStartDate,''AsOfDate'' AsOfDate,''TotalMarketValue'' TotalMarketValue,''TotalCash'' TotalCash,''PerformancePeriod'' PerformancePeriod,''ROR'' ROR,''GrossOrNetOfFees'' GrossOrNetOfFees,''AccountType'' AccountType,''Account_Subtype_Description'' Account_Subtype_Description,''Portfolio_Model_Name'' Portfolio_Model_Name,''Investment_Objective_Description'' Investment_Objective_Description,''Residence_Region'' Residence_Region,''AdvisorName'' AdvisorName,''MoneyManagerName'' MoneyManagerName,''CorrespondenceFirmName'' CorrespondenceFirmName,''last_update'' last_update,''MyStrategyID'' MyStrategyID
DECLARE @block1 varchar(max)
set @block1 = 'select * from (select '' RowID'' RowID,''RelationshipCode'' RelationshipCode,''RelationshipTitle'' RelationshipTitle,''PortfolioCode'' PortfolioCode,''CustodianAccountNumber'' CustodianAccountNumber,''PortfolioName'' PortfolioName,''ManagerStrategy'' ManagerStrategy,''InceptionDate'' InceptionDate,''MoneyManager'' MoneyManager,''ActivityStartDate'' ActivityStartDate,''AsOfDate'' AsOfDate,''TotalMarketValue'' TotalMarketValue,''TotalCash'' TotalCash,''PerformancePeriod'' PerformancePeriod,''ROR'' ROR,''GrossOrNetOfFees'' GrossOrNetOfFees,''AccountType'' AccountType,''Account_Subtype_Description'' Account_Subtype_Description,''Portfolio_Model_Name'' Portfolio_Model_Name,''Investment_Objective_Description'' Investment_Objective_Description,''Residence_Region'' Residence_Region,''AdvisorName'' AdvisorName,''MoneyManagerName'' MoneyManagerName,''CorrespondenceFirmName'' CorrespondenceFirmName,''last_update'' last_update,''MyStrategyID'' MyStrategyID union select DataAgg.dbo.PadLeft(Convert(varchar(50), RowID),''0'',4) RowID , convert(varchar(100),RelationshipCode) RelationshipCode,convert(varchar(100),RelationshipTitle) RelationshipTitle,convert(varchar(100),PortfolioCode) PortfolioCode,convert(varchar(100),CustodianAccountNumber) CustodianAccountNumber,convert(varchar(100),PortfolioName) PortfolioName,convert(varchar(100),ManagerStrategy) ManagerStrategy,convert(varchar(100),InceptionDate) InceptionDate, convert(varchar(100), MoneyManager) MoneyManager, convert(varchar(100),ActivityStartDate) ActivityStartDate,convert(varchar(100),AsOfDate) AsOfDate,convert(varchar(100),TotalMarketValue) TotalMarketValue,convert(varchar(100),TotalCash) TotalCash,convert(varchar(100),PerformancePeriod) PerformancePeriod,convert(varchar(100),ROR) ROR,convert(varchar(100),GrossOrNetOfFees) GrossOrNetOfFees,convert(varchar(100),AccountType) AccountType,convert(varchar(100),Account_Subtype_Description) Account_Subtype_Description,convert(varchar(100),Portfolio_Model_Name) Portfolio_Model_Name,convert(varchar(100),Investment_Objective_Description) Investment_Objective_Description,convert(varchar(100),Residence_Region) Residence_Region,convert(varchar(100),AdvisorName) AdvisorName,convert(varchar(100),MoneyManagerName) MoneyManagerName,convert(varchar(100),CorrespondenceFirmName) CorrespondenceFirmName,convert(varchar(100),last_update) last_update,convert(varchar(100),MyStrategyID) MyStrategyID from ( select convert(varchar(max),null) Title, ROW_NUMBER() OVER(ORDER BY PortfolioCode ASC) AS RowID, * from DataAgg.dbo.proctable_analysisofbenchmarks_performancebyaccount ) A ) A order by RowID asc '
print '@block1-------------------------------------'
print @block1
print '-------------------------------------'
declare @sql varchar(8000)
select @sql = 'bcp "' + @block1 + '" queryout '+@outputfilepathname_detail+' -c -t"," -T -S ipc-vsql01'
print '@sql-------------------------------------'
print @sql
print '-------------------------------------'
exec master..xp_cmdshell @sql

print @outputfilepathname_detail
-- ==================================================================================================================================
-- This is the SUMMARY
-- ==================================================================================================================================
--set nocount on
if object_id('tempdb..#_201612061430_00') is not null drop table tempdb..#_201612061430_00
if object_id('tempdb..#_201612061430_01') is not null drop table tempdb..#_201612061430_01
if object_id('tempdb..#_201612061430_01a') is not null drop table tempdb..#_201612061430_01a


create table #_201612061430_00
(
	PerformanceType	varchar(50),
	PerfID	varchar(50),
	PerformanceName	varchar(300),
	AsOfDate	varchar(50),
	TotalMarketValue	varchar(50),
	Period	varchar(50),
	CountOfPortfolios	int,
	MEDIAN_ROR	float,
	AVG_ROR	float,
	MIN_ROR	float,
	MAX_ROR	float
) 

insert into #_201612061430_00
exec DataAgg..analysisofbenchmarks_performancemedianstatistics @MyPeriod

select 	PerformanceType	
	,PerfID	
	,PerformanceName	
	,AsOfDate	
	,TotalMarketValue	
	,Period	
	,CountOfPortfolios	
	,convert(numeric(6,3),MEDIAN_ROR) MEDIAN_ROR	
	,convert(numeric(6,3),AVG_ROR) AVG_ROR
	,convert(numeric(6,3),MIN_ROR) MIN_ROR
	,convert(numeric(6,3),MAX_ROR) MAX_ROR
	,GETDATE() Last_Update
into #_201612061430_01
from #_201612061430_00
WHERE PerformanceType like '01%'
order by MEDIAN_ROR DESC

-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- Get AccountSubTypes

if object_id('tempdb..#_201612061430_subtype01') is not null drop table tempdb..#_201612061430_subtype01
if object_id('tempdb..#_201612061430_subtype02') is not null drop table tempdb..#_201612061430_subtype02


--select * 
--from DataAgg.dbo.proctable_analysisofbenchmarks_performancebyaccount
update DataAgg.dbo.proctable_analysisofbenchmarks_performancebyaccount
set Account_Subtype_Description = 'missing UMA attrib'
where isnull(ltrim(rtrim(Account_Subtype_Description)),'') = ''
and len(PortfolioCode) > 7

select *
into #_201612061430_subtype01
from
(
	select distinct ManagerStrategy
		, case when Account_Subtype_Description is null then 'SMA' else Account_Subtype_Description end AccountSubType
	from DataAgg.dbo.proctable_analysisofbenchmarks_performancebyaccount
) A

select *
into #_201612061430_subtype02
from
(
	Select Main.ManagerStrategy,
		   Left(Main.AccountSubType,Len(Main.AccountSubType)-1) As "AccountSubType"
	From
		(
			Select distinct ST2.ManagerStrategy, 
				(
					Select ST1.AccountSubType + '+' AS [text()]
					From #_201612061430_subtype01 ST1
					Where ST1.ManagerStrategy = ST2.ManagerStrategy
					ORDER BY ST1.ManagerStrategy
					For XML PATH ('')
				) [AccountSubType]
			From #_201612061430_subtype01 ST2
		) [Main]
) A
--select count(*) from #_201612061430_subtype01
--select * from #_201612061430_subtype02
-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

select *
into #_201612061430_01a
from
(
	select A.PerformanceType,A.PerfID,A.PerformanceName,B.AccountSubType,A.CountOfPortfolios,A.AsOfDate,A.TotalMarketValue,A.Period,A.MEDIAN_ROR,A.AVG_ROR,A.MIN_ROR,A.MAX_ROR,A.Last_Update
	from 
	(
		select * from #_201612061430_01
	) A
	left outer join
	(
		select * from #_201612061430_subtype02
	) B
	on A.PerformanceName = B.ManagerStrategy
) A

if OBJECT_ID('DataAgg.dbo.proctable_analysisofbenchmarks_performancemedianbymanager') is not null drop table DataAgg.dbo.proctable_analysisofbenchmarks_performancemedianbymanager

select *
into DataAgg.dbo.proctable_analysisofbenchmarks_performancemedianbymanager
from #_201612061430_01a



-- select * from DataAgg.dbo.proctable_analysisofbenchmarks_performancemedianbymanager
-- select * from #_201612061430_01

--declare @datestring varchar(25)
set @datestring = (
	select top 1 *
	from
	(
		select top 1 replace(convert(varchar,AsOfDate,111),'-','') + '-' + replace(convert(varchar,GETDATE(),114),':','') Print_Effective_Date
		from DataAgg.dbo.proctable_analysisofbenchmarks_performancemedianbymanager
	) A
)
--select @datestring

declare @outputfilepathname_summary varchar(300)
set @outputfilepathname_summary = '\\ipc-vsql01\DATA\Batches\prod\Performance\output\PerformanceOfStrategies_'+@datestring+'.csv'

/*
	select *
	from
	(
		select ' RowID' RowID,'PerformanceType' PerformanceType,'PerfID' PerfID,'PerformanceName' PerformanceName,'AsOfDate' AsOfDate,'TotalMarketValue' TotalMarketValue,'Period' Period,'CountOfPortfolios' CountOfPortfolios,'MEDIAN_ROR' MEDIAN_ROR,'AVG_ROR' AVG_ROR,'MIN_ROR' MIN_ROR,'MAX_ROR' MAX_ROR,'Last_Update' Last_Update
		union
		select DataAgg.dbo.PadLeft(Convert(varchar(50), RowID),'0',4) RowID , Convert(varchar(50), PerformanceType) PerformanceType, Convert(varchar(50), PerfID) PerfID, Convert(varchar(50), PerformanceName) PerformanceName, Convert(varchar(50), AsOfDate) AsOfDate, Convert(varchar(50), TotalMarketValue) TotalMarketValue, Convert(varchar(50), Period) Period, Convert(varchar(50), CountOfPortfolios) CountOfPortfolios, Convert(varchar(50), MEDIAN_ROR) MEDIAN_ROR, Convert(varchar(50), AVG_ROR) AVG_ROR, Convert(varchar(50), MIN_ROR) MIN_ROR, Convert(varchar(50), MAX_ROR) MAX_ROR, Convert(varchar(50), Last_Update) Last_Update
		from
		(
			select convert(varchar(max),null) Title, ROW_NUMBER() OVER(ORDER BY MEDIAN_ROR DESC) AS RowID, * from DataAgg.dbo.proctable_analysisofbenchmarks_performancemedianbymanager
			
		) A
	) A
	order by RowID asc
	
	
	select *
	from
	(
		select ' RowID' RowID,'PerformanceType' PerformanceType,'PerfID' PerfID,'PerformanceName' PerformanceName,'AsOfDate' AsOfDate,'TotalMarketValue' TotalMarketValue,'Period' Period,'CountOfPortfolios' CountOfPortfolios,'MEDIAN_ROR' MEDIAN_ROR,'AVG_ROR' AVG_ROR,'MIN_ROR' MIN_ROR,'MAX_ROR' MAX_ROR,'Last_Update' Last_Update
		union
		select DataAgg.dbo.PadLeft(Convert(varchar(50), RowID),'0',4) RowID , Convert(varchar(50), PerformanceType) PerformanceType, Convert(varchar(50), PerfID) PerfID, Convert(varchar(50), PerformanceName) PerformanceName, Convert(varchar(50), AsOfDate) AsOfDate, Convert(varchar(50), TotalMarketValue) TotalMarketValue, Convert(varchar(50), Period) Period, Convert(varchar(50), CountOfPortfolios) CountOfPortfolios, Convert(varchar(50), MEDIAN_ROR) MEDIAN_ROR, Convert(varchar(50), AVG_ROR) AVG_ROR, Convert(varchar(50), MIN_ROR) MIN_ROR, Convert(varchar(50), MAX_ROR) MAX_ROR, Convert(varchar(50), Last_Update) Last_Update
		from
		(
			select convert(varchar(max),null) Title, ROW_NUMBER() OVER(ORDER BY MEDIAN_ROR DESC) AS RowID, * from DataAgg.dbo.proctable_analysisofbenchmarks_performancemedianbymanager
			
		) A
	) A
	order by RowID asc
	
	
	

select * from (select ' RowID' RowID,'PerformanceType' PerformanceType,'PerfID' PerfID,'PerformanceName' PerformanceName,'AsOfDate' AsOfDate,'TotalMarketValue' TotalMarketValue,'Period' Period,'CountOfPortfolios' CountOfPortfolios,'MEDIAN_ROR' MEDIAN_ROR,'AVG_ROR' AVG_ROR,'MIN_ROR' MIN_ROR,'MAX_ROR' MAX_ROR,'Last_Update' Last_Update union select DataAgg.dbo.PadLeft(Convert(varchar(50), RowID),'0',4) RowID , Convert(varchar(50), PerformanceType) PerformanceType, Convert(varchar(50), PerfID) PerfID, Convert(varchar(50), PerformanceName) PerformanceName, Convert(varchar(50), AsOfDate) AsOfDate, Convert(varchar(50), TotalMarketValue) TotalMarketValue, Convert(varchar(50), Period) Period, Convert(varchar(50), CountOfPortfolios) CountOfPortfolios, Convert(varchar(50), MEDIAN_ROR) MEDIAN_ROR, Convert(varchar(50), AVG_ROR) AVG_ROR, Convert(varchar(50), MIN_ROR) MIN_ROR, Convert(varchar(50), MAX_ROR) MAX_ROR, Convert(varchar(50), Last_Update) Last_Update from ( select convert(varchar(max),null) Title, ROW_NUMBER() OVER(ORDER BY MEDIAN_ROR DESC) AS RowID, * from 
DataAgg.dbo.proctable_analysisofbenchmarks_performancemedianbymanager ) A ) A order by RowID asc 

select * from (select ' RowID' RowID,'PerformanceType' PerformanceType,'AccountSubType' AccountSubType,'PerfID' PerfID,'PerformanceName' PerformanceName,'AsOfDate' AsOfDate,'TotalMarketValue' TotalMarketValue,'Period' Period,'CountOfPortfolios' CountOfPortfolios,'MEDIAN_ROR' MEDIAN_ROR,'AVG_ROR' AVG_ROR,'MIN_ROR' MIN_ROR,'MAX_ROR' MAX_ROR,'Last_Update' Last_Update 
union 

select DataAgg.dbo.PadLeft(Convert(varchar(50), RowID),'0',4) RowID 
	, Convert(varchar(50), PerformanceType) PerformanceType
	, convert(varchar(50), AccountSubType) AccountSubType
	, Convert(varchar(50), PerfID) PerfID, Convert(varchar(50), PerformanceName) PerformanceName
	, Convert(varchar(50), AsOfDate) AsOfDate
	, Convert(varchar(50), TotalMarketValue) TotalMarketValue
	, Convert(varchar(50), Period) Period
	, Convert(varchar(50), CountOfPortfolios) CountOfPortfolios
	, Convert(varchar(50), MEDIAN_ROR) MEDIAN_ROR
	, Convert(varchar(50), AVG_ROR) AVG_ROR
	, Convert(varchar(50), MIN_ROR) MIN_ROR
	, Convert(varchar(50), MAX_ROR) MAX_ROR
	, Convert(varchar(50), Last_Update) Last_Update 
	from 
	( 
		select convert(varchar(max),null) Title, ROW_NUMBER() OVER(ORDER BY MEDIAN_ROR DESC) AS RowID, * from DataAgg.dbo.proctable_analysisofbenchmarks_performancemedianbymanager 
	) A 
) A order by RowID asc

select * from (select ' RowID' RowID,'PerformanceType' PerformanceType,'AccountSubType' AccountSubType,'PerfID' PerfID,'PerformanceName' PerformanceName,'AsOfDate' AsOfDate,'TotalMarketValue' TotalMarketValue,'Period' Period,'CountOfPortfolios' CountOfPortfolios,'MEDIAN_ROR' MEDIAN_ROR,'AVG_ROR' AVG_ROR,'MIN_ROR' MIN_ROR,'MAX_ROR' MAX_ROR,'Last_Update' Last_Update union select DataAgg.dbo.PadLeft(Convert(varchar(50), RowID),'0',4) RowID , Convert(varchar(50), PerformanceType) PerformanceType, convert(varchar(50), AccountSubType) AccountSubType,Convert(varchar(50), PerfID) PerfID, Convert(varchar(50), PerformanceName) PerformanceName, Convert(varchar(50), AsOfDate) AsOfDate, Convert(varchar(50), TotalMarketValue) TotalMarketValue, Convert(varchar(50), Period) Period, Convert(varchar(50), CountOfPortfolios) CountOfPortfolios, Convert(varchar(50), MEDIAN_ROR) MEDIAN_ROR, Convert(varchar(50), AVG_ROR) AVG_ROR, Convert(varchar(50), MIN_ROR) MIN_ROR, Convert(varchar(50), MAX_ROR) MAX_ROR, Convert(varchar(50), Last_Update) Last_Update from ( select convert(varchar(max),null) Title, ROW_NUMBER() OVER(ORDER BY MEDIAN_ROR DESC) AS RowID, * from DataAgg.dbo.proctable_analysisofbenchmarks_performancemedianbymanager ) A ) A order by RowID asc 

select * from DataAgg.dbo.proctable_analysisofbenchmarks_performancemedianbymanager 

select * from (select ' RowID' RowID,'PerformanceType' PerformanceType,'AccountSubType' AccountSubType,'PerfID' PerfID,'PerformanceName' PerformanceName,'AsOfDate' AsOfDate,'TotalMarketValue' TotalMarketValue,'Period' Period,'CountOfPortfolios' CountOfPortfolios,'MEDIAN_ROR' MEDIAN_ROR,'AVG_ROR' AVG_ROR,'MIN_ROR' MIN_ROR,'MAX_ROR' MAX_ROR,'Last_Update' Last_Update union select DataAgg.dbo.PadLeft(Convert(varchar(50), RowID),'0',4) RowID , Convert(varchar(50), PerformanceType) PerformanceType, convert(varchar(50), AccountSubType) AccountSubType,Convert(varchar(50), PerfID) PerfID, Convert(varchar(50), PerformanceName) PerformanceName, Convert(varchar(50), AsOfDate) AsOfDate, Convert(varchar(50), TotalMarketValue) TotalMarketValue, Convert(varchar(50), Period) Period, Convert(varchar(50), CountOfPortfolios) CountOfPortfolios, Convert(varchar(50), MEDIAN_ROR) MEDIAN_ROR, Convert(varchar(50), AVG_ROR) AVG_ROR, Convert(varchar(50), MIN_ROR) MIN_ROR, Convert(varchar(50), MAX_ROR) MAX_ROR, Convert(varchar(50), Last_Update) Last_Update from ( select convert(varchar(max),null) Title, ROW_NUMBER() OVER(ORDER BY MEDIAN_ROR DESC) AS RowID, * from DataAgg.dbo.proctable_analysisofbenchmarks_performancemedianbymanager ) A ) A order by RowID asc 

*/
--DECLARE @block1 varchar(max)
set @block1 = 'select * from (select '' RowID'' RowID,''PerformanceType'' PerformanceType,''AccountSubType'' AccountSubType,''PerfID'' PerfID,''PerformanceName'' PerformanceName,''AsOfDate'' AsOfDate,''TotalMarketValue'' TotalMarketValue,''Period'' Period,''CountOfPortfolios'' CountOfPortfolios,''MEDIAN_ROR'' MEDIAN_ROR,''AVG_ROR'' AVG_ROR,''MIN_ROR'' MIN_ROR,''MAX_ROR'' MAX_ROR,''Last_Update'' Last_Update union select DataAgg.dbo.PadLeft(Convert(varchar(50), RowID),''0'',4) RowID , Convert(varchar(50), PerformanceType) PerformanceType, convert(varchar(50), AccountSubType) AccountSubType,Convert(varchar(50), PerfID) PerfID, Convert(varchar(50), PerformanceName) PerformanceName, Convert(varchar(50), AsOfDate) AsOfDate, Convert(varchar(50), TotalMarketValue) TotalMarketValue, Convert(varchar(50), Period) Period, Convert(varchar(50), CountOfPortfolios) CountOfPortfolios, Convert(varchar(50), MEDIAN_ROR) MEDIAN_ROR, Convert(varchar(50), AVG_ROR) AVG_ROR, Convert(varchar(50), MIN_ROR) MIN_ROR, Convert(varchar(50), MAX_ROR) MAX_ROR, Convert(varchar(50), Last_Update) Last_Update from ( select convert(varchar(max),null) Title, ROW_NUMBER() OVER(ORDER BY MEDIAN_ROR DESC) AS RowID, * from DataAgg.dbo.proctable_analysisofbenchmarks_performancemedianbymanager ) A ) A order by RowID asc '
--declare @sql varchar(8000)
select @sql = 'bcp "' + @block1 + '" queryout '+@outputfilepathname_summary+' -c -t"," -T -S ipc-vsql01'
print @sql
exec master..xp_cmdshell @sql


--select * from #_201612061430_01

declare @BodyText nvarchar(max)
set  @BodyText = ''

declare @TotalMarketValueString varchar(20)

DECLARE @i int
set @i = 0

DECLARE @PerformanceName varchar(300),@AccountSubType varchar(30), @CountOfPortfolios int,@AsOfDate varchar(12),@TotalMarketValue Float,@Period varchar(10),@MEDIAN_ROR float,@Last_Update datetime
-- ----------------------------------------------------------++ Top 10
-- Start Cursor Top 10
	DECLARE @getTop10Strategies CURSOR
	SET @getTop10Strategies = CURSOR FOR
		 select top 10 PerformanceName,AccountSubType,CountOfPortfolios,AsOfDate,TotalMarketValue,Period,MEDIAN_ROR,Last_Update
		 from DataAgg.dbo.proctable_analysisofbenchmarks_performancemedianbymanager
		 where 1 = 1
		 and not (
				PerformanceName like   'The Boston Company Asset Mgt LLC LGAMV3 - MACON%'
			   or PerformanceName like 'John Hancock Asset Mgt (MFC) MFCLC1 - FUNDAMENTAL%' 
			   or PerformanceName like 'Logan Capital Mgt Inc LCMLG2 - GROWTH%'
			   or PerformanceName like 'SouthernSun Asset Mgt SSASMI - MACON%'
			   or PerformanceName like 'Tributary Capital Mgt TCMSV4  Small Cap Value%'
			   )
			
		 
		 order by convert(float,MEDIAN_ROR) desc
	OPEN @getTop10Strategies
	FETCH NEXT
	FROM @getTop10Strategies INTO @PerformanceName,@AccountSubType,@CountOfPortfolios,@AsOfDate,@TotalMarketValue,@Period,@MEDIAN_ROR,@Last_Update
	WHILE @@FETCH_STATUS = 0
	BEGIN
	 --PRINT @PerformanceName
	 set @i = @i + 1
	 --print @i
	 if @i = 1
		set  @BodyText = @BodyText + '
Top 10 IPC Strategies By Median Performance 
'+@Period+' As Of ' + @AsOfDate + ''
	 set @TotalMarketValueString = DataAgg.dbo.PadLeft('$' +REPLACE(CONVERT(varchar(20), (CAST(convert(numeric(10,0),isnull(@TotalMarketValue,0)) AS money)), 1), '.00', ''),' ',12) 
	 print @TotalMarketValueString
	 if right(@TotalMarketValueString,2) = '$0'
		--print 'convert this! ' + @TotalMarketValueString
		set @TotalMarketValueString = 'not available'
	 
	 set  @BodyText = @BodyText + '
 ' + DataAgg.dbo.PadLeft(CONVERT(VARCHAR,@i)+')',' ',4) + CHAR(9)+DataAgg.dbo.PadLeft(CONVERT(VARCHAR,ROUND(CONVERT(NUMERIC(6,3),isnull(@MEDIAN_ROR,0)),3)),' ',7) + '%' + 
	 CHAR(9)+CHAR(9)+ DataAgg.dbo.PadRight(left(@AccountSubType,12),' ',12) +
	 CHAR(9)+CHAR(9)+ DataAgg.dbo.PadRight(left(@PerformanceName,50),' ',50) +
	 CHAR(9)+CHAR(9)+ DataAgg.dbo.Padleft(@TotalMarketValueString,' ' ,14) + ''
	-- CHAR(9)+CHAR(9)+DataAgg.dbo.PadLeft('$' +REPLACE(CONVERT(varchar(20), isnull(CAST(convert(numeric(10,0),@TotalMarketValue) AS money),'not avail')), 1), '.00', ''),' ',12) 
	 --print convert(varchar,@i) + '  ' + @BodyText
	FETCH NEXT
	FROM @getTop10Strategies INTO @PerformanceName,@AccountSubType,@CountOfPortfolios,@AsOfDate,@TotalMarketValue,@Period,@MEDIAN_ROR,@Last_Update
	END
	 CLOSE @getTop10Strategies
	DEALLOCATE @getTop10Strategies
-- end cursor Top 10
-- ---------------------------------------------------------- --Top 10
print @BodyText
print '----------1'
set @i = 0

-- select * from DataAgg.dbo.proctable_analysisofbenchmarks_performancemedianbymanager
-- ----------------------------------------------------------++ Bottom 10
-- Start Cursor Bottom 10
	DECLARE @getBottom10Strategies CURSOR
	SET @getBottom10Strategies = CURSOR FOR
		--select *
		--from
		--(
		 select top 10 PerformanceName,AccountSubType,CountOfPortfolios,AsOfDate,TotalMarketValue,Period,MEDIAN_ROR,Last_Update
		 from DataAgg.dbo.proctable_analysisofbenchmarks_performancemedianbymanager
		 where not MEDIAN_ROR is null
		 and not (
				PerformanceName like   'The Boston Company Asset Mgt LLC LGAMV3 - MACON%'
			   or PerformanceName like 'John Hancock Asset Mgt (MFC) MFCLC1 - FUNDAMENTAL%' 
			   or PerformanceName like 'Logan Capital Mgt Inc LCMLG2 - GROWTH%'
			   or PerformanceName like 'SouthernSun Asset Mgt SSASMI - MACON%'
			   or PerformanceName like 'Tributary Capital Mgt TCMSV4  Small Cap Value%'
			   )
		 order by convert(float,MEDIAN_ROR) asc
		--) A
		--order by convert(float,MEDIAN_ROR) desc
	OPEN @getBottom10Strategies
	FETCH NEXT
	FROM @getBottom10Strategies INTO @PerformanceName,@AccountSubType,@CountOfPortfolios,@AsOfDate,@TotalMarketValue,@Period,@MEDIAN_ROR,@Last_Update
	WHILE @@FETCH_STATUS = 0
	BEGIN
	 --PRINT @PerformanceName
	 set @i = @i + 1
	 --print @i
	 if @i = 1
		set  @BodyText = @BodyText + '

Bottom 10 IPC Strategies By Median Performance
'+@Period+' As Of ' + @AsOfDate + ''
	 set @TotalMarketValueString = DataAgg.dbo.PadLeft('$' +REPLACE(CONVERT(varchar(20), (CAST(convert(numeric(10,0),isnull(@TotalMarketValue,0)) AS money)), 1), '.00', ''),' ',12) 
	 print @TotalMarketValueString
	 if right(@TotalMarketValueString,2) = '$0'
		--print 'convert this! ' + @TotalMarketValueString
		set @TotalMarketValueString = 'not available'
	 
	 set  @BodyText = @BodyText + '
 ' + DataAgg.dbo.PadLeft(CONVERT(VARCHAR,@i)+')',' ',4) + CHAR(9)+DataAgg.dbo.PadLeft(CONVERT(VARCHAR,ROUND(CONVERT(NUMERIC(6,3),isnull(@MEDIAN_ROR,0)),3)),' ',7) + '%' + 
	 --' + CONVERT(VARCHAR,@i)+')' + CHAR(9)+DataAgg.dbo.PadLeft(CONVERT(VARCHAR,ROUND(CONVERT(NUMERIC(6,3),isnull(@MEDIAN_ROR,0)),3)),' ',7) + '%' + 
	 CHAR(9)+CHAR(9)+ DataAgg.dbo.PadRight(left(@AccountSubType,12),' ',12) +
	 CHAR(9)+CHAR(9)+ DataAgg.dbo.PadRight(left(@PerformanceName,50),' ',50) +
	 CHAR(9)+CHAR(9)+ DataAgg.dbo.Padleft(@TotalMarketValueString,' ' ,14) + ''
	-- CHAR(9)+CHAR(9)+DataAgg.dbo.PadLeft('$' +REPLACE(CONVERT(varchar(20), isnull(CAST(convert(numeric(10,0),@TotalMarketValue) AS money),'not avail')), 1), '.00', ''),' ',12) 
	 --print convert(varchar,@i) + '  ' + @BodyText
	FETCH NEXT
	FROM @getBottom10Strategies INTO @PerformanceName,@AccountSubType,@CountOfPortfolios,@AsOfDate,@TotalMarketValue,@Period,@MEDIAN_ROR,@Last_Update
	END
	 CLOSE @getBottom10Strategies
	DEALLOCATE @getBottom10Strategies
-- end cursor
-- ----------------------------------------------------------++
print @BodyText
print '----------2'

PRINT @outputfilepathname_summary
-- ==================================================================================================================================


-- +++++++++++++++++++++++++++++++++++++++++++++++++++++
-- Insert General Counts Here
-- select * FROM  DataAgg.dbo.proctable_analysisofbenchmarks_performancebyaccount
-- select SUM(TotalMarketValue) FROM  DataAgg.dbo.proctable_analysisofbenchmarks_performancebyaccount

declare @TotalAUM float
set @TotalAUM = (select SUM(TotalMarketValue) FROM  DataAgg.dbo.proctable_analysisofbenchmarks_performancebyaccount)


declare @CountOfAccounts int
set @CountOfAccounts = (select count(*) from DataAgg.dbo.proctable_analysisofbenchmarks_performancebyaccount)

declare @CountOfRelationships int
set @CountOfRelationships = (
		select count(*) 
		from 
		(
			select distinct RelationshipCode FROM  DataAgg.dbo.proctable_analysisofbenchmarks_performancebyaccount
			where RelationshipCode is not null	
		) a	
	)



declare @CountOfStrategies int
set @CountOfStrategies = ( select count(*) from DataAgg.dbo.proctable_analysisofbenchmarks_performancemedianbymanager )

-- +++++++++++++++++++++++++++++++++++++++++++++++++++++




set @BodyText = @BodyText + '

As Of ' + @AsOfDate + '' +'
-----------------------
Total AUM = ' + '$' +REPLACE(CONVERT(varchar(20), (CAST(convert(numeric(10,0),isnull(@TotalAUM,0)) AS money)), 1), '.00', '') + '
Total Strategies = ' + convert(varchar,@CountOfStrategies) + '
Total Portfolios = ' + convert(varchar,@CountOfAccounts) + '
Total Relationships = ' + convert(varchar,@CountOfRelationships) + '

Source: IPC''s Global Wealth Platform & Sylvan Performance
'


declare @IntroBodyText nvarchar(max)
set @IntroBodyText = '
IPC Concierge Top & Bottom Strategies by Median Performance
'+@Period+' As Of ' + @AsOfDate + '' +'
-----------------------------------------------------------------------------------
'	 
set @BodyText = @IntroBodyText + @BodyText

print @BodyText
--declare @myfile_attachments varchar(1000)
--set @myfile_attachments = @outputfilepathname_summary+';'+@outputfilepathname_detail


EXEC sp_send_dbmail @profile_name='IPC Mail Profile',
--@recipients='justin.malinchak@ipcanswers.com',
@recipients='justin.malinchak@ipcanswers.com;david.stone@ipcanswers.com;arun.kaul@ipcanswers.com;john.motherwell@ipcanswers.com;daniel.heeren@ipcanswers.com;edward.kemm@ipcanswers.com;lillian.wong@blueshores.com;katherine.cathcart@ipcanswers.com;victor.sanders@ipcanswers.com;andrew.king@ipcanswers.com',
@subject='Performance by Strategy',
@body=@BodyText,
--@body_format = 'HTML',
@file_attachments=@myfilename


