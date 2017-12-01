--Generated Upgrade For Version 0.0.0.0.54
--Generated on 2017-03-09 14:21:11

--ADD COLUMN [RepositoryStat].[ReadLockCount]
if exists(select * from sys.objects where name = 'RepositoryStat' and type = 'U') AND not exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = 'ReadLockCount' and o.name = 'RepositoryStat')
ALTER TABLE [dbo].[RepositoryStat] ADD [ReadLockCount] [Int] NOT NULL CONSTRAINT [DF__REPOSITORYSTAT_READLOCKCOUNT] DEFAULT (0)

GO

