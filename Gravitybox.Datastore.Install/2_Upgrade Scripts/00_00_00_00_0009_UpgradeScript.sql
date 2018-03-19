--Generated Upgrade For Version 0.0.0.0.9
--Generated on 2014-06-09 08:11:58

--BEGIN DEFAULTS FOR TABLE [UserAccount]
if not exists(select * from sys.objects where name = 'DF__USERACCOUNT_UNIQUEKEY' and type = 'D' and type_desc = 'DEFAULT_CONSTRAINT')
ALTER TABLE [dbo].[UserAccount] ADD CONSTRAINT [DF__USERACCOUNT_UNIQUEKEY] DEFAULT (newid()) FOR [UniqueKey]
GO
