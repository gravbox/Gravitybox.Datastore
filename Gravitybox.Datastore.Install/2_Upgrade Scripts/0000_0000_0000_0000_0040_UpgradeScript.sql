--Generated Upgrade For Version 0.0.0.0.40
--Generated on 2015-04-30 10:58:10

--ADD COLUMN [LockStat].[TraceInfo]
if exists(select * from sys.objects where name = 'LockStat' and type = 'U') AND not exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = 'TraceInfo' and o.name = 'LockStat')
ALTER TABLE [dbo].[LockStat] ADD [TraceInfo] [VarChar] (50) NULL 

GO

