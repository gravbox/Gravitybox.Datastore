--Generated Upgrade For Version 0.0.0.0.56
--Generated on 2017-03-21 13:17:47

--ADD COLUMN [RepositoryLog].[LockTime]
if exists(select * from sys.objects where name = 'RepositoryLog' and type = 'U') AND not exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = 'LockTime' and o.name = 'RepositoryLog')
ALTER TABLE [dbo].[RepositoryLog] ADD [LockTime] [Int] NOT NULL CONSTRAINT [DF__REPOSITORYLOG_LOCKTIME] DEFAULT (0)

GO

--ADD PK
if not exists(select * from sys.objects where name = 'PK_REPOSITORYLOG' and type = 'PK')
ALTER TABLE [dbo].[RepositoryLog] ADD CONSTRAINT PK_REPOSITORYLOG PRIMARY KEY CLUSTERED ([RepositoryLogId])
GO

--ADD PK
if not exists(select * from sys.objects where name = 'PK_REPOSITORYSTAT' and type = 'PK')
ALTER TABLE [dbo].[RepositoryStat] ADD CONSTRAINT PK_REPOSITORYSTAT PRIMARY KEY CLUSTERED ([RepositoryStatId])
GO

--ADD PK
if not exists(select * from sys.objects where name = 'PK_SERVERSTAT' and type = 'PK')
ALTER TABLE [dbo].[ServerStat] ADD CONSTRAINT PK_SERVERSTAT PRIMARY KEY CLUSTERED ([ServerStatId])
GO
