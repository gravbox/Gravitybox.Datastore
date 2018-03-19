--Generated Upgrade For Version 0.0.0.0.6
--Generated on 2014-06-07 13:54:58

--DELETE UNIQUE CONTRAINT
if exists(select * from sysobjects where name = 'IX_DIMENSION_DIDX' and xtype = 'UQ')
ALTER TABLE [Dimension] DROP CONSTRAINT [IX_DIMENSION_DIDX]

--DELETE INDEX
if exists (select * from sys.indexes where name = 'IDX_DIMENSION_DIDX')
DROP INDEX [IDX_DIMENSION_DIDX] ON [Dimension]

DECLARE @defaultName varchar(max)
SET @defaultName = (SELECT d.name FROM sys.columns c inner join sys.default_constraints d on c.column_id = d.parent_column_id and c.object_id = d.parent_object_id inner join sys.objects o on d.parent_object_id = o.object_id where o.name = 'Dimension' and c.name = 'DIdx')
if @defaultName IS NOT NULL
exec('ALTER TABLE [Dimension] DROP CONSTRAINT ' + @defaultName)
GO

--NOTE: IF YOU HAVE AN NON-MANAGED DEFAULT, UNCOMMENT THIS CODE TO REMOVE IT
--DROP CONSTRAINT FOR '[Dimension].[DIdx]' if one exists
--declare @Dimension_DIdx varchar(500)
--set @Dimension_DIdx = (select top 1 c.name from sys.all_columns a inner join sys.tables b on a.object_id = b.object_id inner join sys.default_constraints c on a.default_object_id = c.object_id where b.name='Dimension' and a.name = 'DIdx')
--if (@Dimension_DIdx IS NOT NULL) exec ('ALTER TABLE [Dimension] DROP CONSTRAINT [' + @Dimension_DIdx + ']')
if exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = 'DIdx' and o.name = 'Dimension')
BEGIN

--UPDATE COLUMN
ALTER TABLE [dbo].[Dimension] ALTER COLUMN [DIdx] [BigInt] NOT NULL

END

GO

--ADD COLUMN [DimensionValue].[Value]
if exists(select * from sys.objects where name = 'DimensionValue' and type = 'U') AND not exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = 'Value' and o.name = 'DimensionValue')
ALTER TABLE [dbo].[DimensionValue] ADD [Value] [VarChar] (500) NOT NULL 

GO

