--Generated Upgrade For Version 0.0.0.0.43
--Generated on 2016-04-21 11:19:36

--ADD COLUMN [ServerStat].[CachedItems]
if exists(select * from sys.objects where name = 'ServerStat' and type = 'U') AND not exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = 'CachedItems' and o.name = 'ServerStat')
ALTER TABLE [dbo].[ServerStat] ADD [CachedItems] [Int] NOT NULL CONSTRAINT [DF__SERVERSTAT_CACHEDITEMS] DEFAULT (0)

GO

