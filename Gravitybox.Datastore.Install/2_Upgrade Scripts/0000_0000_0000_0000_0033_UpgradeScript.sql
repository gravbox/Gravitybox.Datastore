--Generated Upgrade For Version 0.0.0.0.33
--Generated on 2014-11-03 16:44:49

--ADD COLUMN [Lock].[RepositoryId]
if exists(select * from sys.objects where name = 'Lock' and type = 'U') AND not exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = 'RepositoryId' and o.name = 'Lock')
ALTER TABLE [dbo].[Lock] ADD [RepositoryId] [Int] NOT NULL 

GO

