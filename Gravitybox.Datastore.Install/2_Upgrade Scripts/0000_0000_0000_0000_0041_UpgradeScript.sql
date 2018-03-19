--Generated Upgrade For Version 0.0.0.0.41
--Generated on 2015-04-30 20:02:02

--INDEX FOR TABLE [LockStat] COLUMNS:[Failure]
if not exists(select * from sys.indexes where name = 'IDX_LOCKSTAT_FAILURE')
CREATE NONCLUSTERED INDEX [IDX_LOCKSTAT_FAILURE] ON [dbo].[LockStat] ([Failure] ASC)

GO

--INDEX FOR TABLE [Repository] COLUMNS:[ParentId]
if not exists(select * from sys.indexes where name = 'IDX_REPOSITORY_PARENTID')
CREATE NONCLUSTERED INDEX [IDX_REPOSITORY_PARENTID] ON [dbo].[Repository] ([ParentId] ASC)

GO

