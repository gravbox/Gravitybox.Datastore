--Generated Upgrade For Version 0.0.0.0.26
--Generated on 2014-06-19 17:08:53

--ADD COLUMN [Repository].[Changestamp]
if exists(select * from sys.objects where name = 'Repository' and type = 'U') AND not exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = 'Changestamp' and o.name = 'Repository')
ALTER TABLE [dbo].[Repository] ADD [Changestamp] [Int] NOT NULL CONSTRAINT [DF__REPOSITORY_CHANGESTAMP] DEFAULT (0)

GO

