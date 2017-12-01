--DO NOT MODIFY THIS FILE. IT IS ALWAYS OVERWRITTEN ON GENERATION.
--Data Schema

--CREATE TABLE [AppliedPatch]
if not exists(select * from sysobjects where name = 'AppliedPatch' and xtype = 'U')
CREATE TABLE [dbo].[AppliedPatch] (
	[ID] [UniqueIdentifier] NOT NULL CONSTRAINT [DF__APPLIEDPATCH_ID] DEFAULT (newid()),
	[Description] [NVarChar] (50) NULL ,
	[ModifiedBy] [NVarchar] (50) NULL,
	[ModifiedDate] [DateTime2] CONSTRAINT [DF__APPLIEDPATCH_MODIFIEDDATE] DEFAULT sysdatetime() NULL,
	[CreatedBy] [NVarchar] (50) NULL,
	[CreatedDate] [DateTime2] CONSTRAINT [DF__APPLIEDPATCH_CREATEDDATE] DEFAULT sysdatetime() NULL,
	[Timestamp] [ROWVERSION] NOT NULL,
	CONSTRAINT [PK_APPLIEDPATCH] PRIMARY KEY CLUSTERED
	(
		[ID]
	)
)

GO

--CREATE TABLE [CacheInvalidate]
if not exists(select * from sysobjects where name = 'CacheInvalidate' and xtype = 'U')
CREATE TABLE [dbo].[CacheInvalidate] (
	[RowId] [BigInt] IDENTITY (1, 1) NOT NULL ,
	[RepositoryId] [Int] NOT NULL ,
	[AddedDate] [DateTime2] (2) NOT NULL CONSTRAINT [DF__CACHEINVALIDATE_ADDEDDATE] DEFAULT (GetDate()),
	[Count] [Int] NOT NULL CONSTRAINT [DF__CACHEINVALIDATE_COUNT] DEFAULT (0),
	CONSTRAINT [PK_CACHEINVALIDATE] PRIMARY KEY CLUSTERED
	(
		[RowId]
	)
)

GO

--CREATE TABLE [ConfigurationSetting]
if not exists(select * from sysobjects where name = 'ConfigurationSetting' and xtype = 'U')
CREATE TABLE [dbo].[ConfigurationSetting] (
	[ID] [Int] IDENTITY (1, 1) NOT NULL ,
	[Name] [NVarChar] (50) NOT NULL ,
	[Value] [NVarChar] (max) NOT NULL ,
	[Timestamp] [ROWVERSION] NOT NULL,
	CONSTRAINT [PK_CONFIGURATIONSETTING] PRIMARY KEY CLUSTERED
	(
		[ID]
	)
)

GO

--CREATE TABLE [Housekeeping]
if not exists(select * from sysobjects where name = 'Housekeeping' and xtype = 'U')
CREATE TABLE [dbo].[Housekeeping] (
	[ID] [Int] IDENTITY (1, 1) NOT NULL ,
	[Data] [VarBinary] (max) NOT NULL ,
	[Type] [Int] NOT NULL ,
	[Timestamp] [ROWVERSION] NOT NULL,
	CONSTRAINT [PK_HOUSEKEEPING] PRIMARY KEY CLUSTERED
	(
		[ID]
	)
)

GO

--CREATE TABLE [Lock]
if not exists(select * from sysobjects where name = 'Lock' and xtype = 'U')
CREATE TABLE [dbo].[Lock] (
	[LockId] [Int] IDENTITY (1, 1) NOT NULL ,
	[MachineId] [Int] NOT NULL ,
	[LockTime] [DateTime] NOT NULL ,
	[IsRead] [Bit] NOT NULL ,
	[Intention] [Bit] NOT NULL ,
	[RepositoryId] [Int] NOT NULL ,
	CONSTRAINT [PK_LOCK] PRIMARY KEY CLUSTERED
	(
		[LockId]
	)
)

GO

--CREATE TABLE [LockStat]
if not exists(select * from sysobjects where name = 'LockStat' and xtype = 'U')
CREATE TABLE [dbo].[LockStat] (
	[LockStatId] [Int] IDENTITY (1, 1) NOT NULL ,
	[ThreadId] [Int] NOT NULL ,
	[Failure] [Bit] NOT NULL ,
	[Elapsed] [Int] NOT NULL ,
	[CurrentReadCount] [Int] NOT NULL ,
	[WaitingReadCount] [Int] NOT NULL ,
	[WaitingWriteCount] [Int] NOT NULL ,
	[IsWriteLockHeld] [Bit] NOT NULL ,
	[DateStamp] [DateTime] NOT NULL ,
	[TraceInfo] [VarChar] (50) NULL ,
	CONSTRAINT [PK_LOCKSTAT] PRIMARY KEY CLUSTERED
	(
		[LockStatId]
	)
)

GO

--CREATE TABLE [Machine]
if not exists(select * from sysobjects where name = 'Machine' and xtype = 'U')
CREATE TABLE [dbo].[Machine] (
	[MachineId] [Int] IDENTITY (1, 1) NOT NULL ,
	[Name] [NVarChar] (100) NOT NULL ,
	[LastCommunication] [DateTime] NOT NULL CONSTRAINT [DF__MACHINE_LASTCOMMUNICATION] DEFAULT (sysdatetime()),
	CONSTRAINT [PK_MACHINE] PRIMARY KEY CLUSTERED
	(
		[MachineId]
	)
)

GO

--CREATE TABLE [Repository]
if not exists(select * from sysobjects where name = 'Repository' and xtype = 'U')
CREATE TABLE [dbo].[Repository] (
	[RepositoryId] [Int] IDENTITY (1, 1) NOT NULL ,
	[ItemCount] [Int] NOT NULL ,
	[UniqueKey] [UniqueIdentifier] NOT NULL ,
	[Name] [NVarChar] (50) NOT NULL ,
	[VersionHash] [BigInt] NOT NULL ,
	[IsDeleted] [Bit] NOT NULL CONSTRAINT [DF__REPOSITORY_ISDELETED] DEFAULT (0),
	[IsInitialized] [Bit] NOT NULL CONSTRAINT [DF__REPOSITORY_ISINITIALIZED] DEFAULT (1),
	[Changestamp] [Int] NOT NULL CONSTRAINT [DF__REPOSITORY_CHANGESTAMP] DEFAULT (0),
	[Dimensionstamp] [Int] NOT NULL CONSTRAINT [DF__REPOSITORY_DIMENSIONSTAMP] DEFAULT (0),
	[DefinitionData] [NVarChar] (max) NULL ,
	[ParentId] [Int] NULL ,
	[ModifiedBy] [NVarchar] (50) NULL,
	[ModifiedDate] [DateTime2] CONSTRAINT [DF__REPOSITORY_MODIFIEDDATE] DEFAULT sysdatetime() NULL,
	[CreatedBy] [NVarchar] (50) NULL,
	[CreatedDate] [DateTime2] CONSTRAINT [DF__REPOSITORY_CREATEDDATE] DEFAULT sysdatetime() NULL,
	[Timestamp] [ROWVERSION] NOT NULL,
	CONSTRAINT [PK_REPOSITORY] PRIMARY KEY CLUSTERED
	(
		[RepositoryId]
	)
)

GO

--CREATE TABLE [RepositoryActionType]
if not exists(select * from sysobjects where name = 'RepositoryActionType' and xtype = 'U')
CREATE TABLE [dbo].[RepositoryActionType] (
	[RepositoryActionTypeId] [Int] IDENTITY (1, 1) NOT NULL ,
	[Name] [VarChar] (50) NOT NULL ,
	CONSTRAINT [PK_REPOSITORYACTIONTYPE] PRIMARY KEY CLUSTERED
	(
		[RepositoryActionTypeId]
	)
)

GO

--CREATE TABLE [RepositoryLog]
if not exists(select * from sysobjects where name = 'RepositoryLog' and xtype = 'U')
CREATE TABLE [dbo].[RepositoryLog] (
	[RepositoryLogId] [BigInt] IDENTITY (1, 1) NOT NULL ,
	[IPAddress] [VarChar] (50) NOT NULL ,
	[RepositoryId] [Int] NOT NULL ,
	[Count] [Int] NOT NULL ,
	[ElapsedTime] [Int] NOT NULL ,
	[UsedCache] [Bit] NOT NULL ,
	[LockTime] [Int] NOT NULL CONSTRAINT [DF__REPOSITORYLOG_LOCKTIME] DEFAULT (0),
	[QueryId] [UniqueIdentifier] NOT NULL ,
	[Query] [NVarChar] (500) NULL ,
	[CreatedBy] [NVarchar] (50) NULL,
	[CreatedDate] [DateTime2] CONSTRAINT [DF__REPOSITORYLOG_CREATEDDATE] DEFAULT sysdatetime() NULL,
	CONSTRAINT [PK_REPOSITORYLOG] PRIMARY KEY CLUSTERED
	(
		[RepositoryLogId]
	)
)

GO

--CREATE TABLE [RepositoryStat]
if not exists(select * from sysobjects where name = 'RepositoryStat' and xtype = 'U')
CREATE TABLE [dbo].[RepositoryStat] (
	[RepositoryStatId] [BigInt] IDENTITY (1, 1) NOT NULL ,
	[RepositoryId] [Int] NOT NULL ,
	[Elapsed] [Int] NOT NULL ,
	[RepositoryActionTypeId] [Int] NOT NULL ,
	[Count] [Int] NOT NULL ,
	[ItemCount] [Int] NOT NULL ,
	[LockTime] [Int] NOT NULL CONSTRAINT [DF__REPOSITORYSTAT_LOCKTIME] DEFAULT (0),
	[WaitingLocks] [Int] NOT NULL CONSTRAINT [DF__REPOSITORYSTAT_WAITINGLOCKS] DEFAULT (0),
	[ReadLockCount] [Int] NOT NULL CONSTRAINT [DF__REPOSITORYSTAT_READLOCKCOUNT] DEFAULT (0),
	[CreatedBy] [NVarchar] (50) NULL,
	[CreatedDate] [DateTime2] CONSTRAINT [DF__REPOSITORYSTAT_CREATEDDATE] DEFAULT sysdatetime() NULL,
	CONSTRAINT [PK_REPOSITORYSTAT] PRIMARY KEY CLUSTERED
	(
		[RepositoryStatId]
	)
)

GO

--CREATE TABLE [Server]
if not exists(select * from sysobjects where name = 'Server' and xtype = 'U')
CREATE TABLE [dbo].[Server] (
	[ServerId] [Int] IDENTITY (1, 1) NOT NULL ,
	[Name] [NVarChar] (50) NOT NULL ,
	[ModifiedBy] [NVarchar] (50) NULL,
	[ModifiedDate] [DateTime2] CONSTRAINT [DF__SERVER_MODIFIEDDATE] DEFAULT sysdatetime() NULL,
	[CreatedBy] [NVarchar] (50) NULL,
	[CreatedDate] [DateTime2] CONSTRAINT [DF__SERVER_CREATEDDATE] DEFAULT sysdatetime() NULL,
	[Timestamp] [ROWVERSION] NOT NULL,
	CONSTRAINT [PK_SERVER] PRIMARY KEY CLUSTERED
	(
		[ServerId]
	)
)

GO

--CREATE TABLE [ServerStat]
if not exists(select * from sysobjects where name = 'ServerStat' and xtype = 'U')
CREATE TABLE [dbo].[ServerStat] (
	[ServerStatId] [BigInt] IDENTITY (1, 1) NOT NULL ,
	[MemoryUsageTotal] [BigInt] NOT NULL ,
	[MemoryUsageAvailable] [BigInt] NOT NULL ,
	[RepositoryLoadDelta] [Int] NOT NULL ,
	[RepositoryUnloadDelta] [Int] NOT NULL ,
	[RepositoryTotal] [Int] NOT NULL ,
	[RepositoryCreateDelta] [BigInt] NOT NULL ,
	[RepositoryDeleteDelta] [BigInt] NOT NULL ,
	[ProcessorUsage] [Int] NOT NULL ,
	[AddedDate] [DateTime] NOT NULL CONSTRAINT [DF__SERVERSTAT_ADDEDDATE] DEFAULT (sysdatetime()),
	[MemoryUsageProcess] [BigInt] NOT NULL ,
	[ServerId] [Int] NOT NULL ,
	[CachedItems] [Int] NOT NULL CONSTRAINT [DF__SERVERSTAT_CACHEDITEMS] DEFAULT (0),
	CONSTRAINT [PK_SERVERSTAT] PRIMARY KEY CLUSTERED
	(
		[ServerStatId]
	)
)

GO

--CREATE TABLE [UserAccount]
if not exists(select * from sysobjects where name = 'UserAccount' and xtype = 'U')
CREATE TABLE [dbo].[UserAccount] (
	[UserId] [Int] IDENTITY (1, 1) NOT NULL ,
	[UniqueKey] [UniqueIdentifier] NOT NULL CONSTRAINT [DF__USERACCOUNT_UNIQUEKEY] DEFAULT (newid()),
	[UserName] [NVarChar] (50) NOT NULL ,
	[Password] [NVarChar] (50) NOT NULL ,
	[ModifiedBy] [NVarchar] (50) NULL,
	[ModifiedDate] [DateTime2] CONSTRAINT [DF__USERACCOUNT_MODIFIEDDATE] DEFAULT sysdatetime() NULL,
	[CreatedBy] [NVarchar] (50) NULL,
	[CreatedDate] [DateTime2] CONSTRAINT [DF__USERACCOUNT_CREATEDDATE] DEFAULT sysdatetime() NULL,
	[Timestamp] [ROWVERSION] NOT NULL,
	CONSTRAINT [PK_USERACCOUNT] PRIMARY KEY CLUSTERED
	(
		[UserId]
	)
)

GO

--##SECTION BEGIN [AUDIT TABLES PK]

--##SECTION END [AUDIT TABLES PK]

--FOREIGN KEY RELATIONSHIP [Machine] -> [Lock] ([Machine].[MachineId] -> [Lock].[MachineId])
if not exists(select * from sysobjects where name = 'FK__LOCK_MACHINE' and xtype = 'F')
ALTER TABLE [dbo].[Lock] ADD 
CONSTRAINT [FK__LOCK_MACHINE] FOREIGN KEY 
(
	[MachineId]
) REFERENCES [dbo].[Machine] (
	[MachineId]
)
GO

--FOREIGN KEY RELATIONSHIP [RepositoryActionType] -> [RepositoryStat] ([RepositoryActionType].[RepositoryActionTypeId] -> [RepositoryStat].[RepositoryActionTypeId])
if not exists(select * from sysobjects where name = 'FK__REPOSITORYSTAT_REPOSITORYACTIONTYPE' and xtype = 'F')
ALTER TABLE [dbo].[RepositoryStat] ADD 
CONSTRAINT [FK__REPOSITORYSTAT_REPOSITORYACTIONTYPE] FOREIGN KEY 
(
	[RepositoryActionTypeId]
) REFERENCES [dbo].[RepositoryActionType] (
	[RepositoryActionTypeId]
)
GO

--FOREIGN KEY RELATIONSHIP [Server] -> [ServerStat] ([Server].[ServerId] -> [ServerStat].[ServerId])
if not exists(select * from sysobjects where name = 'FK__SERVERSTAT_SERVER' and xtype = 'F')
ALTER TABLE [dbo].[ServerStat] ADD 
CONSTRAINT [FK__SERVERSTAT_SERVER] FOREIGN KEY 
(
	[ServerId]
) REFERENCES [dbo].[Server] (
	[ServerId]
)
GO

--##SECTION BEGIN [CREATE INDEXES]

--DELETE INDEX
if exists(select * from sys.indexes where name = 'IDX_CACHEINVALIDATE_REPOSITORYID' and type_desc = 'CLUSTERED')
DROP INDEX [IDX_CACHEINVALIDATE_REPOSITORYID] ON [dbo].[CacheInvalidate]
GO

--INDEX FOR TABLE [CacheInvalidate] COLUMNS:[RepositoryId]
if not exists(select * from sys.indexes where name = 'IDX_CACHEINVALIDATE_REPOSITORYID') and exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = 'RepositoryId' and o.name = 'CacheInvalidate')
CREATE NONCLUSTERED INDEX [IDX_CACHEINVALIDATE_REPOSITORYID] ON [dbo].[CacheInvalidate] ([RepositoryId] ASC)
GO

--DELETE INDEX
if exists(select * from sys.indexes where name = 'IDX_CACHEINVALIDATE_ADDEDDATE' and type_desc = 'CLUSTERED')
DROP INDEX [IDX_CACHEINVALIDATE_ADDEDDATE] ON [dbo].[CacheInvalidate]
GO

--INDEX FOR TABLE [CacheInvalidate] COLUMNS:[AddedDate]
if not exists(select * from sys.indexes where name = 'IDX_CACHEINVALIDATE_ADDEDDATE') and exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = 'AddedDate' and o.name = 'CacheInvalidate')
CREATE NONCLUSTERED INDEX [IDX_CACHEINVALIDATE_ADDEDDATE] ON [dbo].[CacheInvalidate] ([AddedDate] ASC)
GO

--DELETE INDEX
if exists(select * from sys.indexes where name = 'IDX_LOCKSTAT_FAILURE' and type_desc = 'CLUSTERED')
DROP INDEX [IDX_LOCKSTAT_FAILURE] ON [dbo].[LockStat]
GO

--INDEX FOR TABLE [LockStat] COLUMNS:[Failure]
if not exists(select * from sys.indexes where name = 'IDX_LOCKSTAT_FAILURE') and exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = 'Failure' and o.name = 'LockStat')
CREATE NONCLUSTERED INDEX [IDX_LOCKSTAT_FAILURE] ON [dbo].[LockStat] ([Failure] ASC)
GO

--DELETE INDEX
if exists(select * from sys.indexes where name = 'IDX_REPOSITORY_UNIQUEKEY' and type_desc = 'CLUSTERED')
DROP INDEX [IDX_REPOSITORY_UNIQUEKEY] ON [dbo].[Repository]
GO

--INDEX FOR TABLE [Repository] COLUMNS:[UniqueKey]
if not exists(select * from sys.indexes where name = 'IDX_REPOSITORY_UNIQUEKEY') and exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = 'UniqueKey' and o.name = 'Repository')
CREATE NONCLUSTERED INDEX [IDX_REPOSITORY_UNIQUEKEY] ON [dbo].[Repository] ([UniqueKey] ASC)
GO

--DELETE INDEX
if exists(select * from sys.indexes where name = 'IDX_REPOSITORY_ISDELETED' and type_desc = 'CLUSTERED')
DROP INDEX [IDX_REPOSITORY_ISDELETED] ON [dbo].[Repository]
GO

--INDEX FOR TABLE [Repository] COLUMNS:[IsDeleted]
if not exists(select * from sys.indexes where name = 'IDX_REPOSITORY_ISDELETED') and exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = 'IsDeleted' and o.name = 'Repository')
CREATE NONCLUSTERED INDEX [IDX_REPOSITORY_ISDELETED] ON [dbo].[Repository] ([IsDeleted] ASC)
GO

--DELETE INDEX
if exists(select * from sys.indexes where name = 'IDX_REPOSITORY_ISINITIALIZED' and type_desc = 'CLUSTERED')
DROP INDEX [IDX_REPOSITORY_ISINITIALIZED] ON [dbo].[Repository]
GO

--INDEX FOR TABLE [Repository] COLUMNS:[IsInitialized]
if not exists(select * from sys.indexes where name = 'IDX_REPOSITORY_ISINITIALIZED') and exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = 'IsInitialized' and o.name = 'Repository')
CREATE NONCLUSTERED INDEX [IDX_REPOSITORY_ISINITIALIZED] ON [dbo].[Repository] ([IsInitialized] ASC)
GO

--DELETE INDEX
if exists(select * from sys.indexes where name = 'IDX_REPOSITORY_PARENTID' and type_desc = 'CLUSTERED')
DROP INDEX [IDX_REPOSITORY_PARENTID] ON [dbo].[Repository]
GO

--INDEX FOR TABLE [Repository] COLUMNS:[ParentId]
if not exists(select * from sys.indexes where name = 'IDX_REPOSITORY_PARENTID') and exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = 'ParentId' and o.name = 'Repository')
CREATE NONCLUSTERED INDEX [IDX_REPOSITORY_PARENTID] ON [dbo].[Repository] ([ParentId] ASC)
GO

--DELETE INDEX
if exists(select * from sys.indexes where name = 'IDX_REPOSITORYLOG_REPOSITORYID' and type_desc = 'CLUSTERED')
DROP INDEX [IDX_REPOSITORYLOG_REPOSITORYID] ON [dbo].[RepositoryLog]
GO

--INDEX FOR TABLE [RepositoryLog] COLUMNS:[RepositoryId]
if not exists(select * from sys.indexes where name = 'IDX_REPOSITORYLOG_REPOSITORYID') and exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = 'RepositoryId' and o.name = 'RepositoryLog')
CREATE NONCLUSTERED INDEX [IDX_REPOSITORYLOG_REPOSITORYID] ON [dbo].[RepositoryLog] ([RepositoryId] ASC)
GO

--DELETE INDEX
if exists(select * from sys.indexes where name = 'IDX_REPOSITORYSTAT_REPOSITORYACTIONTYPEID' and type_desc = 'CLUSTERED')
DROP INDEX [IDX_REPOSITORYSTAT_REPOSITORYACTIONTYPEID] ON [dbo].[RepositoryStat]
GO

--INDEX FOR TABLE [RepositoryStat] COLUMNS:[RepositoryActionTypeId]
if not exists(select * from sys.indexes where name = 'IDX_REPOSITORYSTAT_REPOSITORYACTIONTYPEID') and exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = 'RepositoryActionTypeId' and o.name = 'RepositoryStat')
CREATE NONCLUSTERED INDEX [IDX_REPOSITORYSTAT_REPOSITORYACTIONTYPEID] ON [dbo].[RepositoryStat] ([RepositoryActionTypeId] ASC)
GO

--DELETE INDEX
if exists(select * from sys.indexes where name = 'IDX_SERVERSTAT_SERVERID' and type_desc = 'CLUSTERED')
DROP INDEX [IDX_SERVERSTAT_SERVERID] ON [dbo].[ServerStat]
GO

--INDEX FOR TABLE [ServerStat] COLUMNS:[ServerId]
if not exists(select * from sys.indexes where name = 'IDX_SERVERSTAT_SERVERID') and exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = 'ServerId' and o.name = 'ServerStat')
CREATE NONCLUSTERED INDEX [IDX_SERVERSTAT_SERVERID] ON [dbo].[ServerStat] ([ServerId] ASC)
GO

--##SECTION END [CREATE INDEXES]

--##SECTION BEGIN [TENANT INDEXES]

--##SECTION END [TENANT INDEXES]

if not exists(select * from sys.objects where [name] = '__nhydrateschema' and [type] = 'U')
BEGIN
CREATE TABLE [__nhydrateschema] (
[dbVersion] [varchar] (50) NOT NULL,
[LastUpdate] [datetime] NOT NULL,
[ModelKey] [uniqueidentifier] NOT NULL,
[History] [nvarchar](max) NOT NULL
)
if not exists(select * from sys.objects where [name] = '__pk__nhydrateschema' and [type] = 'PK')
ALTER TABLE [__nhydrateschema] WITH NOCHECK ADD CONSTRAINT [__pk__nhydrateschema] PRIMARY KEY CLUSTERED ([ModelKey])
END
GO

if not exists(select * from sys.objects where name = '__nhydrateobjects' and [type] = 'U')
CREATE TABLE [dbo].[__nhydrateobjects]
(
	[rowid] [bigint] IDENTITY(1,1) NOT NULL,
	[id] [uniqueidentifier] NULL,
	[name] [nvarchar](450) NOT NULL,
	[type] [varchar](10) NOT NULL,
	[schema] [nvarchar](450) NULL,
	[CreatedDate] [datetime] NOT NULL,
	[ModifiedDate] [datetime] NOT NULL,
	[Hash] [varchar](32) NULL,
	[ModelKey] [uniqueidentifier] NOT NULL,
)

if not exists(select * from sys.indexes where name = '__ix__nhydrateobjects_name')
CREATE NONCLUSTERED INDEX [__ix__nhydrateobjects_name] ON [dbo].[__nhydrateobjects]
(
	[name] ASC
)

if not exists(select * from sys.indexes where name = '__ix__nhydrateobjects_schema')
CREATE NONCLUSTERED INDEX [__ix__nhydrateobjects_schema] ON [dbo].[__nhydrateobjects] 
(
	[schema] ASC
)

if not exists(select * from sys.indexes where name = '__ix__nhydrateobjects_type')
CREATE NONCLUSTERED INDEX [__ix__nhydrateobjects_type] ON [dbo].[__nhydrateobjects] 
(
	[type] ASC
)

if not exists(select * from sys.indexes where name = '__ix__nhydrateobjects_modelkey')
CREATE NONCLUSTERED INDEX [__ix__nhydrateobjects_modelkey] ON [dbo].[__nhydrateobjects] 
(
	[ModelKey] ASC
)

if not exists(select * from sys.indexes where name = '__pk__nhydrateobjects')
ALTER TABLE [dbo].[__nhydrateobjects] ADD CONSTRAINT [__pk__nhydrateobjects] PRIMARY KEY CLUSTERED 
(
	[rowid] ASC
)
GO

