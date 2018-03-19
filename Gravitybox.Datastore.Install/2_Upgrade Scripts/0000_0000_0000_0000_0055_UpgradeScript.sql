--Generated Upgrade For Version 0.0.0.0.55
--Generated on 2017-03-09 14:35:29

--DROP PK BECAUSE THE MODIFIED FIELD IS A PK COLUMN
if exists(select * from sys.objects where name = 'PK_REPOSITORYLOG' and type = 'PK')
ALTER TABLE [dbo].[RepositoryLog] DROP CONSTRAINT PK_REPOSITORYLOG
GO

--DELETE UNIQUE CONTRAINT
if exists(select * from sysobjects where name = 'IX_REPOSITORYLOG_REPOSITORYLOGID' and xtype = 'UQ')
ALTER TABLE [RepositoryLog] DROP CONSTRAINT [IX_REPOSITORYLOG_REPOSITORYLOGID]

--DELETE INDEX
if exists (select * from sys.indexes where name = 'IDX_REPOSITORYLOG_REPOSITORYLOGID')
DROP INDEX [IDX_REPOSITORYLOG_REPOSITORYLOGID] ON [RepositoryLog]

DECLARE @defaultName varchar(max)
SET @defaultName = (SELECT d.name FROM sys.columns c inner join sys.default_constraints d on c.column_id = d.parent_column_id and c.object_id = d.parent_object_id inner join sys.objects o on d.parent_object_id = o.object_id where o.name = 'RepositoryLog' and c.name = 'RepositoryLogId')
if @defaultName IS NOT NULL
exec('ALTER TABLE [RepositoryLog] DROP CONSTRAINT ' + @defaultName)
GO

--NOTE: IF YOU HAVE AN NON-MANAGED DEFAULT, UNCOMMENT THIS CODE TO REMOVE IT
--DROP CONSTRAINT FOR '[RepositoryLog].[RepositoryLogId]' if one exists
--declare @RepositoryLog_RepositoryLogId varchar(500)
--set @RepositoryLog_RepositoryLogId = (select top 1 c.name from sys.all_columns a inner join sys.tables b on a.object_id = b.object_id inner join sys.default_constraints c on a.default_object_id = c.object_id where b.name='RepositoryLog' and a.name = 'RepositoryLogId')
--if (@RepositoryLog_RepositoryLogId IS NOT NULL) exec ('ALTER TABLE [RepositoryLog] DROP CONSTRAINT [' + @RepositoryLog_RepositoryLogId + ']')
if exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = 'RepositoryLogId' and o.name = 'RepositoryLog')
BEGIN

--UPDATE COLUMN
ALTER TABLE [dbo].[RepositoryLog] ALTER COLUMN [RepositoryLogId] [BigInt] NOT NULL

END

GO

--DROP PK BECAUSE THE MODIFIED FIELD IS A PK COLUMN
if exists(select * from sys.objects where name = 'PK_REPOSITORYSTAT' and type = 'PK')
ALTER TABLE [dbo].[RepositoryStat] DROP CONSTRAINT PK_REPOSITORYSTAT
GO

--DELETE UNIQUE CONTRAINT
if exists(select * from sysobjects where name = 'IX_REPOSITORYSTAT_REPOSITORYSTATID' and xtype = 'UQ')
ALTER TABLE [RepositoryStat] DROP CONSTRAINT [IX_REPOSITORYSTAT_REPOSITORYSTATID]

--DELETE INDEX
if exists (select * from sys.indexes where name = 'IDX_REPOSITORYSTAT_REPOSITORYSTATID')
DROP INDEX [IDX_REPOSITORYSTAT_REPOSITORYSTATID] ON [RepositoryStat]

DECLARE @defaultName varchar(max)
SET @defaultName = (SELECT d.name FROM sys.columns c inner join sys.default_constraints d on c.column_id = d.parent_column_id and c.object_id = d.parent_object_id inner join sys.objects o on d.parent_object_id = o.object_id where o.name = 'RepositoryStat' and c.name = 'RepositoryStatId')
if @defaultName IS NOT NULL
exec('ALTER TABLE [RepositoryStat] DROP CONSTRAINT ' + @defaultName)
GO

--NOTE: IF YOU HAVE AN NON-MANAGED DEFAULT, UNCOMMENT THIS CODE TO REMOVE IT
--DROP CONSTRAINT FOR '[RepositoryStat].[RepositoryStatId]' if one exists
--declare @RepositoryStat_RepositoryStatId varchar(500)
--set @RepositoryStat_RepositoryStatId = (select top 1 c.name from sys.all_columns a inner join sys.tables b on a.object_id = b.object_id inner join sys.default_constraints c on a.default_object_id = c.object_id where b.name='RepositoryStat' and a.name = 'RepositoryStatId')
--if (@RepositoryStat_RepositoryStatId IS NOT NULL) exec ('ALTER TABLE [RepositoryStat] DROP CONSTRAINT [' + @RepositoryStat_RepositoryStatId + ']')
if exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = 'RepositoryStatId' and o.name = 'RepositoryStat')
BEGIN

--UPDATE COLUMN
ALTER TABLE [dbo].[RepositoryStat] ALTER COLUMN [RepositoryStatId] [BigInt] NOT NULL

END

GO

--DROP PK BECAUSE THE MODIFIED FIELD IS A PK COLUMN
if exists(select * from sys.objects where name = 'PK_SERVERSTAT' and type = 'PK')
ALTER TABLE [dbo].[ServerStat] DROP CONSTRAINT PK_SERVERSTAT
GO

--DELETE UNIQUE CONTRAINT
if exists(select * from sysobjects where name = 'IX_SERVERSTAT_SERVERSTATID' and xtype = 'UQ')
ALTER TABLE [ServerStat] DROP CONSTRAINT [IX_SERVERSTAT_SERVERSTATID]

--DELETE INDEX
if exists (select * from sys.indexes where name = 'IDX_SERVERSTAT_SERVERSTATID')
DROP INDEX [IDX_SERVERSTAT_SERVERSTATID] ON [ServerStat]

DECLARE @defaultName varchar(max)
SET @defaultName = (SELECT d.name FROM sys.columns c inner join sys.default_constraints d on c.column_id = d.parent_column_id and c.object_id = d.parent_object_id inner join sys.objects o on d.parent_object_id = o.object_id where o.name = 'ServerStat' and c.name = 'ServerStatId')
if @defaultName IS NOT NULL
exec('ALTER TABLE [ServerStat] DROP CONSTRAINT ' + @defaultName)
GO

--NOTE: IF YOU HAVE AN NON-MANAGED DEFAULT, UNCOMMENT THIS CODE TO REMOVE IT
--DROP CONSTRAINT FOR '[ServerStat].[ServerStatId]' if one exists
--declare @ServerStat_ServerStatId varchar(500)
--set @ServerStat_ServerStatId = (select top 1 c.name from sys.all_columns a inner join sys.tables b on a.object_id = b.object_id inner join sys.default_constraints c on a.default_object_id = c.object_id where b.name='ServerStat' and a.name = 'ServerStatId')
--if (@ServerStat_ServerStatId IS NOT NULL) exec ('ALTER TABLE [ServerStat] DROP CONSTRAINT [' + @ServerStat_ServerStatId + ']')
if exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = 'ServerStatId' and o.name = 'ServerStat')
BEGIN

--UPDATE COLUMN
ALTER TABLE [dbo].[ServerStat] ALTER COLUMN [ServerStatId] [BigInt] NOT NULL

END

GO

