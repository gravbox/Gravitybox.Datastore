--Generated Upgrade For Version 2.1.0.0.61
--Generated on 2017-12-04 13:17:31

--CREATE TABLE [ServiceInstance]
if not exists(select * from sysobjects where name = 'ServiceInstance' and xtype = 'U')
CREATE TABLE [dbo].[ServiceInstance] (
	[RowId] [Int] NOT NULL CONSTRAINT [DF__SERVICEINSTANCE_ROWID] DEFAULT (1),
	[LastCommunication] [DateTime2] (2) NOT NULL ,
	[FirstCommunication] [DateTime2] (2) NOT NULL ,
	[InstanceId] [UniqueIdentifier] NOT NULL ,
	[ModifiedBy] [NVarchar] (50) NULL,
	[ModifiedDate] [DateTime2] CONSTRAINT [DF__SERVICEINSTANCE_MODIFIEDDATE] DEFAULT sysdatetime() NULL,
	[CreatedBy] [NVarchar] (50) NULL,
	[CreatedDate] [DateTime2] CONSTRAINT [DF__SERVICEINSTANCE_CREATEDDATE] DEFAULT sysdatetime() NULL,
	[Timestamp] [ROWVERSION] NOT NULL,
	CONSTRAINT [PK_SERVICEINSTANCE] PRIMARY KEY CLUSTERED
	(
		[RowId]
	)
)

GO
--PRIMARY KEY FOR TABLE [ServiceInstance]
if not exists(select * from sysobjects where name = 'PK_SERVICEINSTANCE' and xtype = 'PK')
ALTER TABLE [dbo].[ServiceInstance] WITH NOCHECK ADD 
CONSTRAINT [PK_SERVICEINSTANCE] PRIMARY KEY CLUSTERED
(
	[RowId]
)

GO
--REMOVE FOREIGN KEY
if exists(select * from sys.objects where name = 'FK__LOCK_MACHINE' and type = 'F' and type_desc = 'FOREIGN_KEY_CONSTRAINT')
ALTER TABLE [dbo].[Lock] DROP CONSTRAINT [FK__LOCK_MACHINE]

--DELETE PRIMARY KEY FOR TABLE [Lock]
if exists(select * from sys.objects where name = 'PK_Lock' and type = 'PK' and type_desc = 'PRIMARY_KEY_CONSTRAINT')
ALTER TABLE [dbo].[Lock] DROP CONSTRAINT [PK_Lock]

--DELETE UNIQUE CONTRAINT
if exists(select * from sysobjects where name = 'IX_LOCK_LOCKID' and xtype = 'UQ')
ALTER TABLE [Lock] DROP CONSTRAINT [IX_LOCK_LOCKID]

--DELETE UNIQUE CONTRAINT
if exists(select * from sysobjects where name = 'IX_LOCK_INTENTION' and xtype = 'UQ')
ALTER TABLE [Lock] DROP CONSTRAINT [IX_LOCK_INTENTION]

--DELETE INDEX
if exists (select * from sys.indexes where name = 'IDX_LOCK_INTENTION')
DROP INDEX [IDX_LOCK_INTENTION] ON [Lock]

--DELETE UNIQUE CONTRAINT
if exists(select * from sysobjects where name = 'IX_LOCK_ISREAD' and xtype = 'UQ')
ALTER TABLE [Lock] DROP CONSTRAINT [IX_LOCK_ISREAD]

--DELETE INDEX
if exists (select * from sys.indexes where name = 'IDX_LOCK_ISREAD')
DROP INDEX [IDX_LOCK_ISREAD] ON [Lock]

--DELETE UNIQUE CONTRAINT
if exists(select * from sysobjects where name = 'IX_LOCK_LOCKTIME' and xtype = 'UQ')
ALTER TABLE [Lock] DROP CONSTRAINT [IX_LOCK_LOCKTIME]

--DELETE INDEX
if exists (select * from sys.indexes where name = 'IDX_LOCK_LOCKTIME')
DROP INDEX [IDX_LOCK_LOCKTIME] ON [Lock]

--DELETE UNIQUE CONTRAINT
if exists(select * from sysobjects where name = 'IX_LOCK_MACHINEID' and xtype = 'UQ')
ALTER TABLE [Lock] DROP CONSTRAINT [IX_LOCK_MACHINEID]

--DELETE INDEX
if exists (select * from sys.indexes where name = 'IDX_LOCK_MACHINEID')
DROP INDEX [IDX_LOCK_MACHINEID] ON [Lock]

--DELETE UNIQUE CONTRAINT
if exists(select * from sysobjects where name = 'IX_LOCK_REPOSITORYID' and xtype = 'UQ')
ALTER TABLE [Lock] DROP CONSTRAINT [IX_LOCK_REPOSITORYID]

--DELETE INDEX
if exists (select * from sys.indexes where name = 'IDX_LOCK_REPOSITORYID')
DROP INDEX [IDX_LOCK_REPOSITORYID] ON [Lock]

--DELETE TABLE [Lock]
if exists (select * from sysobjects where name = 'Lock' and xtype = 'U')
DROP TABLE [Lock]
GO

--REMOVE FOREIGN KEY
if exists(select * from sys.objects where name = 'FK__Lock_Machine' and type = 'F' and type_desc = 'FOREIGN_KEY_CONSTRAINT')
ALTER TABLE [dbo].[Lock] DROP CONSTRAINT [FK__Lock_Machine]

--DELETE PRIMARY KEY FOR TABLE [Machine]
if exists(select * from sys.objects where name = 'PK_Machine' and type = 'PK' and type_desc = 'PRIMARY_KEY_CONSTRAINT')
ALTER TABLE [dbo].[Machine] DROP CONSTRAINT [PK_Machine]

--DELETE UNIQUE CONTRAINT
if exists(select * from sysobjects where name = 'IX_MACHINE_MACHINEID' and xtype = 'UQ')
ALTER TABLE [Machine] DROP CONSTRAINT [IX_MACHINE_MACHINEID]

--DELETE UNIQUE CONTRAINT
if exists(select * from sysobjects where name = 'IX_MACHINE_LASTCOMMUNICATION' and xtype = 'UQ')
ALTER TABLE [Machine] DROP CONSTRAINT [IX_MACHINE_LASTCOMMUNICATION]

--DELETE INDEX
if exists (select * from sys.indexes where name = 'IDX_MACHINE_LASTCOMMUNICATION')
DROP INDEX [IDX_MACHINE_LASTCOMMUNICATION] ON [Machine]

--DELETE UNIQUE CONTRAINT
if exists(select * from sysobjects where name = 'IX_MACHINE_NAME' and xtype = 'UQ')
ALTER TABLE [Machine] DROP CONSTRAINT [IX_MACHINE_NAME]

--DELETE INDEX
if exists (select * from sys.indexes where name = 'IDX_MACHINE_NAME')
DROP INDEX [IDX_MACHINE_NAME] ON [Machine]

--DELETE TABLE [Machine]
if exists (select * from sysobjects where name = 'Machine' and xtype = 'U')
DROP TABLE [Machine]
GO

