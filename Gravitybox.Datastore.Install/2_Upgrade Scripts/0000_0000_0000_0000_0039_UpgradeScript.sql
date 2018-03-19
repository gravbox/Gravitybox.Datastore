--Generated Upgrade For Version 0.0.0.0.39
--Generated on 2015-04-30 10:31:42

--ADD COLUMN [LockStat].[DateStamp]
if exists(select * from sys.objects where name = 'LockStat' and type = 'U') AND not exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = 'DateStamp' and o.name = 'LockStat')
ALTER TABLE [dbo].[LockStat] ADD [DateStamp] [DateTime] NOT NULL 

GO

