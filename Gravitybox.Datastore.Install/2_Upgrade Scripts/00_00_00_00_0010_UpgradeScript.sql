--Generated Upgrade For Version 0.0.0.0.10
--Generated on 2014-06-09 09:47:10

--DELETE DEFAULT
select 'ALTER TABLE [dbo].[Repository] DROP CONSTRAINT ' + [name] as 'sql' into #t from sysobjects where id IN( select SC.cdefault FROM dbo.sysobjects SO INNER JOIN dbo.syscolumns SC ON SO.id = SC.id LEFT JOIN sys.default_constraints SM ON SC.cdefault = SM.parent_column_id WHERE SO.xtype = 'U' and SO.NAME = 'Repository' and SC.NAME = 'MemorySize')
declare @sql [nvarchar] (1000)
SELECT @sql = MAX([sql]) from #t
exec (@sql)
drop table #t

--DELETE UNIQUE CONTRAINT
if exists(select * from sysobjects where name = 'IX_REPOSITORY_MEMORYSIZE' and xtype = 'UQ')
ALTER TABLE [Repository] DROP CONSTRAINT [IX_REPOSITORY_MEMORYSIZE]

--DELETE INDEX
if exists (select * from sys.indexes where name = 'IDX_REPOSITORY_MEMORYSIZE')
DROP INDEX [IDX_REPOSITORY_MEMORYSIZE] ON [Repository]

--DROP COLUMN
if exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = 'MemorySize' and o.name = 'Repository')
ALTER TABLE [dbo].[Repository] DROP COLUMN [MemorySize]

GO

