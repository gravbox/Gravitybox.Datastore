--Generated Upgrade For Version 0.0.0.0.31
--Generated on 2014-10-31 12:41:38

--DELETE UNIQUE CONTRAINT
if exists(select * from sysobjects where name = 'IX_APPLIEDPATCH_DESCRIPTION' and xtype = 'UQ')
ALTER TABLE [AppliedPatch] DROP CONSTRAINT [IX_APPLIEDPATCH_DESCRIPTION]

--DELETE INDEX
if exists (select * from sys.indexes where name = 'IDX_APPLIEDPATCH_DESCRIPTION')
DROP INDEX [IDX_APPLIEDPATCH_DESCRIPTION] ON [AppliedPatch]

DECLARE @defaultName varchar(max)
SET @defaultName = (SELECT d.name FROM sys.columns c inner join sys.default_constraints d on c.column_id = d.parent_column_id and c.object_id = d.parent_object_id inner join sys.objects o on d.parent_object_id = o.object_id where o.name = 'AppliedPatch' and c.name = 'Description')
if @defaultName IS NOT NULL
exec('ALTER TABLE [AppliedPatch] DROP CONSTRAINT ' + @defaultName)
GO

--NOTE: IF YOU HAVE AN NON-MANAGED DEFAULT, UNCOMMENT THIS CODE TO REMOVE IT
--DROP CONSTRAINT FOR '[AppliedPatch].[Description]' if one exists
--declare @AppliedPatch_Description varchar(500)
--set @AppliedPatch_Description = (select top 1 c.name from sys.all_columns a inner join sys.tables b on a.object_id = b.object_id inner join sys.default_constraints c on a.default_object_id = c.object_id where b.name='AppliedPatch' and a.name = 'Description')
--if (@AppliedPatch_Description IS NOT NULL) exec ('ALTER TABLE [AppliedPatch] DROP CONSTRAINT [' + @AppliedPatch_Description + ']')
if exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = 'Description' and o.name = 'AppliedPatch')
BEGIN

--UPDATE COLUMN
ALTER TABLE [dbo].[AppliedPatch] ALTER COLUMN [Description] [NVarChar] (50) NULL

END

GO

--DELETE UNIQUE CONTRAINT
if exists(select * from sysobjects where name = 'IX_CONFIGURATIONSETTING_NAME' and xtype = 'UQ')
ALTER TABLE [ConfigurationSetting] DROP CONSTRAINT [IX_CONFIGURATIONSETTING_NAME]

--DELETE INDEX
if exists (select * from sys.indexes where name = 'IDX_CONFIGURATIONSETTING_NAME')
DROP INDEX [IDX_CONFIGURATIONSETTING_NAME] ON [ConfigurationSetting]

DECLARE @defaultName varchar(max)
SET @defaultName = (SELECT d.name FROM sys.columns c inner join sys.default_constraints d on c.column_id = d.parent_column_id and c.object_id = d.parent_object_id inner join sys.objects o on d.parent_object_id = o.object_id where o.name = 'ConfigurationSetting' and c.name = 'Name')
if @defaultName IS NOT NULL
exec('ALTER TABLE [ConfigurationSetting] DROP CONSTRAINT ' + @defaultName)
GO

--NOTE: IF YOU HAVE AN NON-MANAGED DEFAULT, UNCOMMENT THIS CODE TO REMOVE IT
--DROP CONSTRAINT FOR '[ConfigurationSetting].[Name]' if one exists
--declare @ConfigurationSetting_Name varchar(500)
--set @ConfigurationSetting_Name = (select top 1 c.name from sys.all_columns a inner join sys.tables b on a.object_id = b.object_id inner join sys.default_constraints c on a.default_object_id = c.object_id where b.name='ConfigurationSetting' and a.name = 'Name')
--if (@ConfigurationSetting_Name IS NOT NULL) exec ('ALTER TABLE [ConfigurationSetting] DROP CONSTRAINT [' + @ConfigurationSetting_Name + ']')
if exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = 'Name' and o.name = 'ConfigurationSetting')
BEGIN

--UPDATE COLUMN
ALTER TABLE [dbo].[ConfigurationSetting] ALTER COLUMN [Name] [NVarChar] (50) NOT NULL

END

GO

--DELETE UNIQUE CONTRAINT
if exists(select * from sysobjects where name = 'IX_CONFIGURATIONSETTING_VALUE' and xtype = 'UQ')
ALTER TABLE [ConfigurationSetting] DROP CONSTRAINT [IX_CONFIGURATIONSETTING_VALUE]

--DELETE INDEX
if exists (select * from sys.indexes where name = 'IDX_CONFIGURATIONSETTING_VALUE')
DROP INDEX [IDX_CONFIGURATIONSETTING_VALUE] ON [ConfigurationSetting]

DECLARE @defaultName varchar(max)
SET @defaultName = (SELECT d.name FROM sys.columns c inner join sys.default_constraints d on c.column_id = d.parent_column_id and c.object_id = d.parent_object_id inner join sys.objects o on d.parent_object_id = o.object_id where o.name = 'ConfigurationSetting' and c.name = 'Value')
if @defaultName IS NOT NULL
exec('ALTER TABLE [ConfigurationSetting] DROP CONSTRAINT ' + @defaultName)
GO

--NOTE: IF YOU HAVE AN NON-MANAGED DEFAULT, UNCOMMENT THIS CODE TO REMOVE IT
--DROP CONSTRAINT FOR '[ConfigurationSetting].[Value]' if one exists
--declare @ConfigurationSetting_Value varchar(500)
--set @ConfigurationSetting_Value = (select top 1 c.name from sys.all_columns a inner join sys.tables b on a.object_id = b.object_id inner join sys.default_constraints c on a.default_object_id = c.object_id where b.name='ConfigurationSetting' and a.name = 'Value')
--if (@ConfigurationSetting_Value IS NOT NULL) exec ('ALTER TABLE [ConfigurationSetting] DROP CONSTRAINT [' + @ConfigurationSetting_Value + ']')
if exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = 'Value' and o.name = 'ConfigurationSetting')
BEGIN

--UPDATE COLUMN
ALTER TABLE [dbo].[ConfigurationSetting] ALTER COLUMN [Value] [NVarChar] (max) NOT NULL

END

GO

--DELETE UNIQUE CONTRAINT
if exists(select * from sysobjects where name = 'IX_DIMENSIONVALUE_VALUE' and xtype = 'UQ')
ALTER TABLE [DimensionValue] DROP CONSTRAINT [IX_DIMENSIONVALUE_VALUE]

--DELETE INDEX
if exists (select * from sys.indexes where name = 'IDX_DIMENSIONVALUE_VALUE')
DROP INDEX [IDX_DIMENSIONVALUE_VALUE] ON [DimensionValue]

DECLARE @defaultName varchar(max)
SET @defaultName = (SELECT d.name FROM sys.columns c inner join sys.default_constraints d on c.column_id = d.parent_column_id and c.object_id = d.parent_object_id inner join sys.objects o on d.parent_object_id = o.object_id where o.name = 'DimensionValue' and c.name = 'Value')
if @defaultName IS NOT NULL
exec('ALTER TABLE [DimensionValue] DROP CONSTRAINT ' + @defaultName)
GO

--NOTE: IF YOU HAVE AN NON-MANAGED DEFAULT, UNCOMMENT THIS CODE TO REMOVE IT
--DROP CONSTRAINT FOR '[DimensionValue].[Value]' if one exists
--declare @DimensionValue_Value varchar(500)
--set @DimensionValue_Value = (select top 1 c.name from sys.all_columns a inner join sys.tables b on a.object_id = b.object_id inner join sys.default_constraints c on a.default_object_id = c.object_id where b.name='DimensionValue' and a.name = 'Value')
--if (@DimensionValue_Value IS NOT NULL) exec ('ALTER TABLE [DimensionValue] DROP CONSTRAINT [' + @DimensionValue_Value + ']')
if exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = 'Value' and o.name = 'DimensionValue')
BEGIN

--UPDATE COLUMN
ALTER TABLE [dbo].[DimensionValue] ALTER COLUMN [Value] [NVarChar] (500) NOT NULL

END

GO

--DELETE UNIQUE CONTRAINT
if exists(select * from sysobjects where name = 'IX_REPOSITORY_DEFINITIONDATA' and xtype = 'UQ')
ALTER TABLE [Repository] DROP CONSTRAINT [IX_REPOSITORY_DEFINITIONDATA]

--DELETE INDEX
if exists (select * from sys.indexes where name = 'IDX_REPOSITORY_DEFINITIONDATA')
DROP INDEX [IDX_REPOSITORY_DEFINITIONDATA] ON [Repository]

DECLARE @defaultName varchar(max)
SET @defaultName = (SELECT d.name FROM sys.columns c inner join sys.default_constraints d on c.column_id = d.parent_column_id and c.object_id = d.parent_object_id inner join sys.objects o on d.parent_object_id = o.object_id where o.name = 'Repository' and c.name = 'DefinitionData')
if @defaultName IS NOT NULL
exec('ALTER TABLE [Repository] DROP CONSTRAINT ' + @defaultName)
GO

--NOTE: IF YOU HAVE AN NON-MANAGED DEFAULT, UNCOMMENT THIS CODE TO REMOVE IT
--DROP CONSTRAINT FOR '[Repository].[DefinitionData]' if one exists
--declare @Repository_DefinitionData varchar(500)
--set @Repository_DefinitionData = (select top 1 c.name from sys.all_columns a inner join sys.tables b on a.object_id = b.object_id inner join sys.default_constraints c on a.default_object_id = c.object_id where b.name='Repository' and a.name = 'DefinitionData')
--if (@Repository_DefinitionData IS NOT NULL) exec ('ALTER TABLE [Repository] DROP CONSTRAINT [' + @Repository_DefinitionData + ']')
if exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = 'DefinitionData' and o.name = 'Repository')
BEGIN

--UPDATE COLUMN
ALTER TABLE [dbo].[Repository] ALTER COLUMN [DefinitionData] [NVarChar] (max) NULL

END

GO

--DELETE UNIQUE CONTRAINT
if exists(select * from sysobjects where name = 'IX_REPOSITORY_NAME' and xtype = 'UQ')
ALTER TABLE [Repository] DROP CONSTRAINT [IX_REPOSITORY_NAME]

--DELETE INDEX
if exists (select * from sys.indexes where name = 'IDX_REPOSITORY_NAME')
DROP INDEX [IDX_REPOSITORY_NAME] ON [Repository]

DECLARE @defaultName varchar(max)
SET @defaultName = (SELECT d.name FROM sys.columns c inner join sys.default_constraints d on c.column_id = d.parent_column_id and c.object_id = d.parent_object_id inner join sys.objects o on d.parent_object_id = o.object_id where o.name = 'Repository' and c.name = 'Name')
if @defaultName IS NOT NULL
exec('ALTER TABLE [Repository] DROP CONSTRAINT ' + @defaultName)
GO

--NOTE: IF YOU HAVE AN NON-MANAGED DEFAULT, UNCOMMENT THIS CODE TO REMOVE IT
--DROP CONSTRAINT FOR '[Repository].[Name]' if one exists
--declare @Repository_Name varchar(500)
--set @Repository_Name = (select top 1 c.name from sys.all_columns a inner join sys.tables b on a.object_id = b.object_id inner join sys.default_constraints c on a.default_object_id = c.object_id where b.name='Repository' and a.name = 'Name')
--if (@Repository_Name IS NOT NULL) exec ('ALTER TABLE [Repository] DROP CONSTRAINT [' + @Repository_Name + ']')
if exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = 'Name' and o.name = 'Repository')
BEGIN

--UPDATE COLUMN
ALTER TABLE [dbo].[Repository] ALTER COLUMN [Name] [NVarChar] (50) NOT NULL

END

GO

--DELETE UNIQUE CONTRAINT
if exists(select * from sysobjects where name = 'IX_REPOSITORYLOG_QUERY' and xtype = 'UQ')
ALTER TABLE [RepositoryLog] DROP CONSTRAINT [IX_REPOSITORYLOG_QUERY]

--DELETE INDEX
if exists (select * from sys.indexes where name = 'IDX_REPOSITORYLOG_QUERY')
DROP INDEX [IDX_REPOSITORYLOG_QUERY] ON [RepositoryLog]

DECLARE @defaultName varchar(max)
SET @defaultName = (SELECT d.name FROM sys.columns c inner join sys.default_constraints d on c.column_id = d.parent_column_id and c.object_id = d.parent_object_id inner join sys.objects o on d.parent_object_id = o.object_id where o.name = 'RepositoryLog' and c.name = 'Query')
if @defaultName IS NOT NULL
exec('ALTER TABLE [RepositoryLog] DROP CONSTRAINT ' + @defaultName)
GO

--NOTE: IF YOU HAVE AN NON-MANAGED DEFAULT, UNCOMMENT THIS CODE TO REMOVE IT
--DROP CONSTRAINT FOR '[RepositoryLog].[Query]' if one exists
--declare @RepositoryLog_Query varchar(500)
--set @RepositoryLog_Query = (select top 1 c.name from sys.all_columns a inner join sys.tables b on a.object_id = b.object_id inner join sys.default_constraints c on a.default_object_id = c.object_id where b.name='RepositoryLog' and a.name = 'Query')
--if (@RepositoryLog_Query IS NOT NULL) exec ('ALTER TABLE [RepositoryLog] DROP CONSTRAINT [' + @RepositoryLog_Query + ']')
if exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = 'Query' and o.name = 'RepositoryLog')
BEGIN

--UPDATE COLUMN
ALTER TABLE [dbo].[RepositoryLog] ALTER COLUMN [Query] [NVarChar] (50) NULL

END

GO

--DELETE UNIQUE CONTRAINT
if exists(select * from sysobjects where name = 'IX_SERVER_NAME' and xtype = 'UQ')
ALTER TABLE [Server] DROP CONSTRAINT [IX_SERVER_NAME]

--DELETE INDEX
if exists (select * from sys.indexes where name = 'IDX_SERVER_NAME')
DROP INDEX [IDX_SERVER_NAME] ON [Server]

DECLARE @defaultName varchar(max)
SET @defaultName = (SELECT d.name FROM sys.columns c inner join sys.default_constraints d on c.column_id = d.parent_column_id and c.object_id = d.parent_object_id inner join sys.objects o on d.parent_object_id = o.object_id where o.name = 'Server' and c.name = 'Name')
if @defaultName IS NOT NULL
exec('ALTER TABLE [Server] DROP CONSTRAINT ' + @defaultName)
GO

--NOTE: IF YOU HAVE AN NON-MANAGED DEFAULT, UNCOMMENT THIS CODE TO REMOVE IT
--DROP CONSTRAINT FOR '[Server].[Name]' if one exists
--declare @Server_Name varchar(500)
--set @Server_Name = (select top 1 c.name from sys.all_columns a inner join sys.tables b on a.object_id = b.object_id inner join sys.default_constraints c on a.default_object_id = c.object_id where b.name='Server' and a.name = 'Name')
--if (@Server_Name IS NOT NULL) exec ('ALTER TABLE [Server] DROP CONSTRAINT [' + @Server_Name + ']')
if exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = 'Name' and o.name = 'Server')
BEGIN

--UPDATE COLUMN
ALTER TABLE [dbo].[Server] ALTER COLUMN [Name] [NVarChar] (50) NOT NULL

END

GO

--DELETE UNIQUE CONTRAINT
if exists(select * from sysobjects where name = 'IX_USERACCOUNT_PASSWORD' and xtype = 'UQ')
ALTER TABLE [UserAccount] DROP CONSTRAINT [IX_USERACCOUNT_PASSWORD]

--DELETE INDEX
if exists (select * from sys.indexes where name = 'IDX_USERACCOUNT_PASSWORD')
DROP INDEX [IDX_USERACCOUNT_PASSWORD] ON [UserAccount]

DECLARE @defaultName varchar(max)
SET @defaultName = (SELECT d.name FROM sys.columns c inner join sys.default_constraints d on c.column_id = d.parent_column_id and c.object_id = d.parent_object_id inner join sys.objects o on d.parent_object_id = o.object_id where o.name = 'UserAccount' and c.name = 'Password')
if @defaultName IS NOT NULL
exec('ALTER TABLE [UserAccount] DROP CONSTRAINT ' + @defaultName)
GO

--NOTE: IF YOU HAVE AN NON-MANAGED DEFAULT, UNCOMMENT THIS CODE TO REMOVE IT
--DROP CONSTRAINT FOR '[UserAccount].[Password]' if one exists
--declare @UserAccount_Password varchar(500)
--set @UserAccount_Password = (select top 1 c.name from sys.all_columns a inner join sys.tables b on a.object_id = b.object_id inner join sys.default_constraints c on a.default_object_id = c.object_id where b.name='UserAccount' and a.name = 'Password')
--if (@UserAccount_Password IS NOT NULL) exec ('ALTER TABLE [UserAccount] DROP CONSTRAINT [' + @UserAccount_Password + ']')
if exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = 'Password' and o.name = 'UserAccount')
BEGIN

--UPDATE COLUMN
ALTER TABLE [dbo].[UserAccount] ALTER COLUMN [Password] [NVarChar] (50) NOT NULL

END

GO

--DELETE UNIQUE CONTRAINT
if exists(select * from sysobjects where name = 'IX_USERACCOUNT_USERNAME' and xtype = 'UQ')
ALTER TABLE [UserAccount] DROP CONSTRAINT [IX_USERACCOUNT_USERNAME]

--DELETE INDEX
if exists (select * from sys.indexes where name = 'IDX_USERACCOUNT_USERNAME')
DROP INDEX [IDX_USERACCOUNT_USERNAME] ON [UserAccount]

DECLARE @defaultName varchar(max)
SET @defaultName = (SELECT d.name FROM sys.columns c inner join sys.default_constraints d on c.column_id = d.parent_column_id and c.object_id = d.parent_object_id inner join sys.objects o on d.parent_object_id = o.object_id where o.name = 'UserAccount' and c.name = 'UserName')
if @defaultName IS NOT NULL
exec('ALTER TABLE [UserAccount] DROP CONSTRAINT ' + @defaultName)
GO

--NOTE: IF YOU HAVE AN NON-MANAGED DEFAULT, UNCOMMENT THIS CODE TO REMOVE IT
--DROP CONSTRAINT FOR '[UserAccount].[UserName]' if one exists
--declare @UserAccount_UserName varchar(500)
--set @UserAccount_UserName = (select top 1 c.name from sys.all_columns a inner join sys.tables b on a.object_id = b.object_id inner join sys.default_constraints c on a.default_object_id = c.object_id where b.name='UserAccount' and a.name = 'UserName')
--if (@UserAccount_UserName IS NOT NULL) exec ('ALTER TABLE [UserAccount] DROP CONSTRAINT [' + @UserAccount_UserName + ']')
if exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = 'UserName' and o.name = 'UserAccount')
BEGIN

--UPDATE COLUMN
ALTER TABLE [dbo].[UserAccount] ALTER COLUMN [UserName] [NVarChar] (50) NOT NULL

END

GO

