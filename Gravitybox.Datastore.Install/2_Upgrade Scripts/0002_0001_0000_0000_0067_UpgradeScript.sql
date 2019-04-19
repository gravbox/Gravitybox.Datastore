--Generated Upgrade For Version 2.1.0.0.67
--Generated on 2019-04-17 10:14:20

--ADD COLUMN [CacheInvalidate].[Reason]
if exists(select * from sys.objects where name = 'CacheInvalidate' and type = 'U') AND not exists (select * from sys.columns c inner join sys.objects o on c.object_id = o.object_id where c.name = 'Reason' and o.name = 'CacheInvalidate')
ALTER TABLE [dbo].[CacheInvalidate] ADD [Reason] [VarChar] (20) NULL 

GO

--ADD COLUMN [CacheInvalidate].[Subkey]
if exists(select * from sys.objects where name = 'CacheInvalidate' and type = 'U') AND not exists (select * from sys.columns c inner join sys.objects o on c.object_id = o.object_id where c.name = 'Subkey' and o.name = 'CacheInvalidate')
ALTER TABLE [dbo].[CacheInvalidate] ADD [Subkey] [VarChar] (50) NULL 

GO

