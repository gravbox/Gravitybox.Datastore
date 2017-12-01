--Generated Upgrade For Version 2.1.0.0.60
--Generated on 2017-11-29 14:35:33

--DELETE PRIMARY KEY FOR TABLE [UserAccount]
if exists(select * from sys.objects where name = 'PK_UserAccount' and type = 'PK' and type_desc = 'PRIMARY_KEY_CONSTRAINT')
ALTER TABLE [dbo].[UserAccount] DROP CONSTRAINT [PK_UserAccount]

--DELETE UNIQUE CONTRAINT
if exists(select * from sysobjects where name = 'IX_USERACCOUNT_USERID' and xtype = 'UQ')
ALTER TABLE [UserAccount] DROP CONSTRAINT [IX_USERACCOUNT_USERID]

--DELETE UNIQUE CONTRAINT
if exists(select * from sysobjects where name = 'IX_USERACCOUNT_PASSWORD' and xtype = 'UQ')
ALTER TABLE [UserAccount] DROP CONSTRAINT [IX_USERACCOUNT_PASSWORD]

--DELETE INDEX
if exists (select * from sys.indexes where name = 'IDX_USERACCOUNT_PASSWORD')
DROP INDEX [IDX_USERACCOUNT_PASSWORD] ON [UserAccount]

--DELETE UNIQUE CONTRAINT
if exists(select * from sysobjects where name = 'IX_USERACCOUNT_UNIQUEKEY' and xtype = 'UQ')
ALTER TABLE [UserAccount] DROP CONSTRAINT [IX_USERACCOUNT_UNIQUEKEY]

--DELETE INDEX
if exists (select * from sys.indexes where name = 'IDX_USERACCOUNT_UNIQUEKEY')
DROP INDEX [IDX_USERACCOUNT_UNIQUEKEY] ON [UserAccount]

--DELETE UNIQUE CONTRAINT
if exists(select * from sysobjects where name = 'IX_USERACCOUNT_USERNAME' and xtype = 'UQ')
ALTER TABLE [UserAccount] DROP CONSTRAINT [IX_USERACCOUNT_USERNAME]

--DELETE INDEX
if exists (select * from sys.indexes where name = 'IDX_USERACCOUNT_USERNAME')
DROP INDEX [IDX_USERACCOUNT_USERNAME] ON [UserAccount]

--DELETE TABLE [UserAccount]
if exists (select * from sysobjects where name = 'UserAccount' and xtype = 'U')
DROP TABLE [UserAccount]
GO

