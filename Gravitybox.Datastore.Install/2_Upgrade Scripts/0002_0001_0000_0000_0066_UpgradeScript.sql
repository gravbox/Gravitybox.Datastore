--Generated Upgrade For Version 2.1.0.0.66
--Generated on 2019-03-03 11:46:36

--CREATE TABLE [DeleteQueue]
if not exists(select * from sys.tables t inner join sys.schemas s on t.schema_id = s.schema_id where t.name = 'DeleteQueue' and s.name = 'dbo')
CREATE TABLE [dbo].[DeleteQueue] (
	[RowId] [BigInt] IDENTITY (1, 1) NOT NULL ,
	[RepositoryId] [Int] NOT NULL ,
	[IsReady] [Bit] NOT NULL CONSTRAINT [DF__DELETEQUEUE_ISREADY] DEFAULT (0),
	CONSTRAINT [PK_DELETEQUEUE] PRIMARY KEY CLUSTERED
	(
		[RowId]
	)
)

GO
--PRIMARY KEY FOR TABLE [DeleteQueue]
if not exists(select * from sys.objects where name = 'PK_DELETEQUEUE' and type = 'PK')
ALTER TABLE [dbo].[DeleteQueue] WITH NOCHECK ADD 
CONSTRAINT [PK_DELETEQUEUE] PRIMARY KEY CLUSTERED
(
	[RowId]
)

GO
--CREATE TABLE [DeleteQueueItem]
if not exists(select * from sys.tables t inner join sys.schemas s on t.schema_id = s.schema_id where t.name = 'DeleteQueueItem' and s.name = 'dbo')
CREATE TABLE [dbo].[DeleteQueueItem] (
	[ParentRowId] [BigInt] NOT NULL ,
	[RecordIdx] [BigInt] NOT NULL ,
	CONSTRAINT [PK_DELETEQUEUEITEM] PRIMARY KEY CLUSTERED
	(
		[ParentRowId],[RecordIdx]
	)
)

GO
--PRIMARY KEY FOR TABLE [DeleteQueueItem]
if not exists(select * from sys.objects where name = 'PK_DELETEQUEUEITEM' and type = 'PK')
ALTER TABLE [dbo].[DeleteQueueItem] WITH NOCHECK ADD 
CONSTRAINT [PK_DELETEQUEUEITEM] PRIMARY KEY CLUSTERED
(
	[ParentRowId],[RecordIdx]
)

GO
