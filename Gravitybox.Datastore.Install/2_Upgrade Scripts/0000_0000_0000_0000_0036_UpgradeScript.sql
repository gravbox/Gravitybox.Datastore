--Generated Upgrade For Version 0.0.0.0.36
--Generated on 2015-03-30 11:23:18

--ADD COLUMN [Repository].[ParentId]
if exists(select * from sys.objects where name = 'Repository' and type = 'U') AND not exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = 'ParentId' and o.name = 'Repository')
ALTER TABLE [dbo].[Repository] ADD [ParentId] [Int] NULL 

GO

