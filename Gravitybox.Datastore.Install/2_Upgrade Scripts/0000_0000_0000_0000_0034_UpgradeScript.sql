--Generated Upgrade For Version 0.0.0.0.34
--Generated on 2014-11-21 12:26:00

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
ALTER TABLE [dbo].[RepositoryLog] ALTER COLUMN [Query] [NVarChar] (500) NULL

END

GO

