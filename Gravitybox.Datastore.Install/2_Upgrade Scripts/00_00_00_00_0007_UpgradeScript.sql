--Generated Upgrade For Version 0.0.0.0.7
--Generated on 2014-06-07 15:27:14

--REMOVE FOREIGN KEY
if exists(select * from sys.objects where name = 'FK__REPOSITORYUSER_USERACCOUNT' and type = 'F' and type_desc = 'FOREIGN_KEY_CONSTRAINT')
ALTER TABLE [dbo].[RepositoryUser] DROP CONSTRAINT [FK__REPOSITORYUSER_USERACCOUNT]

--REMOVE FOREIGN KEY
if exists(select * from sys.objects where name = 'FK__REPOSITORYUSER_REPOSITORY' and type = 'F' and type_desc = 'FOREIGN_KEY_CONSTRAINT')
ALTER TABLE [dbo].[RepositoryUser] DROP CONSTRAINT [FK__REPOSITORYUSER_REPOSITORY]

--DELETE PRIMARY KEY FOR TABLE [RepositoryUser]
if exists(select * from sys.objects where name = 'PK_RepositoryUser' and type = 'PK' and type_desc = 'PRIMARY_KEY_CONSTRAINT')
ALTER TABLE [dbo].[RepositoryUser] DROP CONSTRAINT [PK_RepositoryUser]

--DELETE UNIQUE CONTRAINT
if exists(select * from sysobjects where name = 'IX_REPOSITORYUSER_REPOSITORYID' and xtype = 'UQ')
ALTER TABLE [RepositoryUser] DROP CONSTRAINT [IX_REPOSITORYUSER_REPOSITORYID]

--DELETE UNIQUE CONTRAINT
if exists(select * from sysobjects where name = 'IX_REPOSITORYUSER_USERID' and xtype = 'UQ')
ALTER TABLE [RepositoryUser] DROP CONSTRAINT [IX_REPOSITORYUSER_USERID]

--DELETE TABLE [RepositoryUser]
if exists (select * from sysobjects where name = 'RepositoryUser' and xtype = 'U')
DROP TABLE [RepositoryUser]
GO

--REMOVE FOREIGN KEY
if exists(select * from sysobjects where name = 'FK__REPOSITORYUSER_REPOSITORY' and xtype = 'F')
ALTER TABLE [dbo].[RepositoryUser] DROP CONSTRAINT [FK__REPOSITORYUSER_REPOSITORY]
GO

--REMOVE FOREIGN KEY
if exists(select * from sysobjects where name = 'FK__REPOSITORYUSER_USERACCOUNT' and xtype = 'F')
ALTER TABLE [dbo].[RepositoryUser] DROP CONSTRAINT [FK__REPOSITORYUSER_USERACCOUNT]
GO

--INDEX FOR TABLE [Dimension] COLUMNS:[DIdx]
if not exists(select * from sys.indexes where name = 'IDX_DIMENSION_DIDX')
CREATE NONCLUSTERED INDEX [IDX_DIMENSION_DIDX] ON [dbo].[Dimension] ([DIdx] ASC)

GO

--INDEX FOR TABLE [Dimension] COLUMNS:[RepositoryId]
if not exists(select * from sys.indexes where name = 'IDX_DIMENSION_REPOSITORYID')
CREATE NONCLUSTERED INDEX [IDX_DIMENSION_REPOSITORYID] ON [dbo].[Dimension] ([RepositoryId] ASC)

GO

--INDEX FOR TABLE [DimensionValue] COLUMNS:[DimensionId]
if not exists(select * from sys.indexes where name = 'IDX_DIMENSIONVALUE_DIMENSIONID')
CREATE NONCLUSTERED INDEX [IDX_DIMENSIONVALUE_DIMENSIONID] ON [dbo].[DimensionValue] ([DimensionId] ASC)

GO

--INDEX FOR TABLE [Repository] COLUMNS:[UniqueKey]
if not exists(select * from sys.indexes where name = 'IDX_REPOSITORY_UNIQUEKEY')
CREATE NONCLUSTERED INDEX [IDX_REPOSITORY_UNIQUEKEY] ON [dbo].[Repository] ([UniqueKey] ASC)

GO

--INDEX FOR TABLE [RepositoryLog] COLUMNS:[RepositoryId]
if not exists(select * from sys.indexes where name = 'IDX_REPOSITORYLOG_REPOSITORYID')
CREATE NONCLUSTERED INDEX [IDX_REPOSITORYLOG_REPOSITORYID] ON [dbo].[RepositoryLog] ([RepositoryId] ASC)

GO

--INDEX FOR TABLE [RepositoryStat] COLUMNS:[RepositoryActionTypeId]
if not exists(select * from sys.indexes where name = 'IDX_REPOSITORYSTAT_REPOSITORYACTIONTYPEID')
CREATE NONCLUSTERED INDEX [IDX_REPOSITORYSTAT_REPOSITORYACTIONTYPEID] ON [dbo].[RepositoryStat] ([RepositoryActionTypeId] ASC)

GO

--INDEX FOR TABLE [ServerStat] COLUMNS:[ServerId]
if not exists(select * from sys.indexes where name = 'IDX_SERVERSTAT_SERVERID')
CREATE NONCLUSTERED INDEX [IDX_SERVERSTAT_SERVERID] ON [dbo].[ServerStat] ([ServerId] ASC)

GO

