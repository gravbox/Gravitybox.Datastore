--Generated Upgrade For Version 2.1.0.0.62
--Generated on 2017-12-04 14:06:26

--REMOVE AUDIT TRAIL CREATE FOR TABLE [ServiceInstance]
if exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = 'CreatedBy' and o.name = 'ServiceInstance')
ALTER TABLE [dbo].[ServiceInstance] DROP COLUMN [CreatedBy]
if exists (select * from sys.objects where name = 'DF__SERVICEINSTANCE_CREATEDDATE' and [type] = 'D')
ALTER TABLE [dbo].[ServiceInstance] DROP CONSTRAINT [DF__SERVICEINSTANCE_CREATEDDATE]
if exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = 'CreatedDate' and o.name = 'ServiceInstance')
ALTER TABLE [dbo].[ServiceInstance] DROP COLUMN [CreatedDate]
GO

--REMOVE AUDIT TRAIL MODIFY FOR TABLE [ServiceInstance]
if exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = 'ModifiedBy' and o.name = 'ServiceInstance')
ALTER TABLE [dbo].[ServiceInstance] DROP COLUMN [ModifiedBy]
if exists (select * from sys.objects where name = 'DF__SERVICEINSTANCE_MODIFIEDDATE' and [type] = 'D')
ALTER TABLE [dbo].[ServiceInstance] DROP CONSTRAINT [DF__SERVICEINSTANCE_MODIFIEDDATE]
if exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = 'ModifiedDate' and o.name = 'ServiceInstance')
ALTER TABLE [dbo].[ServiceInstance] DROP COLUMN [ModifiedDate]
GO

--REMOVE AUDIT TRAIL TIMESTAMP FOR TABLE [ServiceInstance]
if exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = 'Timestamp' and o.name = 'ServiceInstance')
ALTER TABLE [dbo].[ServiceInstance] DROP COLUMN [Timestamp]
GO

