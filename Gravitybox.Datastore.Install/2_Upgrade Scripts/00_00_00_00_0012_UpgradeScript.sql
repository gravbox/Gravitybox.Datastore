--Generated Upgrade For Version 0.0.0.0.12
--Generated on 2014-06-09 15:59:02

--DELETE DEFAULT
select 'ALTER TABLE [dbo].[ServerStat] DROP CONSTRAINT ' + [name] as 'sql' into #t from sysobjects where id IN( select SC.cdefault FROM dbo.sysobjects SO INNER JOIN dbo.syscolumns SC ON SO.id = SC.id LEFT JOIN sys.default_constraints SM ON SC.cdefault = SM.parent_column_id WHERE SO.xtype = 'U' and SO.NAME = 'ServerStat' and SC.NAME = 'RepositoryInMem')
declare @sql [nvarchar] (1000)
SELECT @sql = MAX([sql]) from #t
exec (@sql)
drop table #t

--DELETE UNIQUE CONTRAINT
if exists(select * from sysobjects where name = 'IX_SERVERSTAT_REPOSITORYINMEM' and xtype = 'UQ')
ALTER TABLE [ServerStat] DROP CONSTRAINT [IX_SERVERSTAT_REPOSITORYINMEM]

--DELETE INDEX
if exists (select * from sys.indexes where name = 'IDX_SERVERSTAT_REPOSITORYINMEM')
DROP INDEX [IDX_SERVERSTAT_REPOSITORYINMEM] ON [ServerStat]

--DROP COLUMN
if exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = 'RepositoryInMem' and o.name = 'ServerStat')
ALTER TABLE [dbo].[ServerStat] DROP COLUMN [RepositoryInMem]

GO

