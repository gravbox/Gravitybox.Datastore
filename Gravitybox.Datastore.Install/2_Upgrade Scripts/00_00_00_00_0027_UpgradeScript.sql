--Generated Upgrade For Version 0.0.0.0.27
--Generated on 2014-06-23 13:38:07

--ADD COLUMN [Repository].[Dimensionstamp]
if exists(select * from sys.objects where name = 'Repository' and type = 'U') AND not exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = 'Dimensionstamp' and o.name = 'Repository')
ALTER TABLE [dbo].[Repository] ADD [Dimensionstamp] [Int] NOT NULL CONSTRAINT [DF__REPOSITORY_DIMENSIONSTAMP] DEFAULT (0)

GO

