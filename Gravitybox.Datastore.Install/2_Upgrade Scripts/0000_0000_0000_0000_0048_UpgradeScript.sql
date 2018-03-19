--Generated Upgrade For Version 0.0.0.0.48
--Generated on 2017-03-01 16:11:38

--ADD COLUMN [RepositoryStat].[WaitingLocks]
if exists(select * from sys.objects where name = 'RepositoryStat' and type = 'U') AND not exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = 'WaitingLocks' and o.name = 'RepositoryStat')
ALTER TABLE [dbo].[RepositoryStat] ADD [WaitingLocks] [Int] NOT NULL CONSTRAINT [DF__REPOSITORYSTAT_WAITINGLOCKS] DEFAULT (0)

GO

