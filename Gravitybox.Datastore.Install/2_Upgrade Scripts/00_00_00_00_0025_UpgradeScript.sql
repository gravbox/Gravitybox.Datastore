--Generated Upgrade For Version 0.0.0.0.25
--Generated on 2014-06-16 09:48:53

--ADD COLUMN [Machine].[LastCommunication]
if exists(select * from sys.objects where name = 'Machine' and type = 'U') AND not exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = 'LastCommunication' and o.name = 'Machine')
ALTER TABLE [dbo].[Machine] ADD [LastCommunication] [DateTime] NOT NULL CONSTRAINT [DF__MACHINE_LASTCOMMUNICATION] DEFAULT (getdate())

GO

