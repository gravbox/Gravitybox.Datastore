--Generated Upgrade For Version 0.0.0.0.46
--Generated on 2016-09-22 14:45:16

--REMOVE FOREIGN KEY
if exists(select * from sysobjects where name = 'FK__REPOSITORYLOG_REPOSITORY' and xtype = 'F')
ALTER TABLE [dbo].[RepositoryLog] DROP CONSTRAINT [FK__REPOSITORYLOG_REPOSITORY]
GO

