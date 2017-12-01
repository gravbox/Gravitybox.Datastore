--Generated Upgrade For Version 0.0.0.0.47
--Generated on 2017-02-26 11:17:02

--ADD COLUMN [RepositoryStat].[LockTime]
if exists(select * from sys.objects where name = 'RepositoryStat' and type = 'U') AND not exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = 'LockTime' and o.name = 'RepositoryStat')
ALTER TABLE [dbo].[RepositoryStat] ADD [LockTime] [Int] NOT NULL CONSTRAINT [DF__REPOSITORYSTAT_LOCKTIME] DEFAULT (0)

GO

