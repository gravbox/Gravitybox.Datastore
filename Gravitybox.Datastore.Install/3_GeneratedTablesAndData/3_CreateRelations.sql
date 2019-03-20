--DO NOT MODIFY THIS FILE. IT IS ALWAYS OVERWRITTEN ON GENERATION.
--Relations

--##SECTION BEGIN [RELATIONS]

--FOREIGN KEY RELATIONSHIP [DeleteQueue] -> [DeleteQueueItem] ([DeleteQueue].[RowId] -> [DeleteQueueItem].[ParentRowId])
if not exists(select * from sys.objects where name = 'FK__DELETEQUEUEITEM_DELETEQUEUE' and type = 'F')
ALTER TABLE [dbo].[DeleteQueueItem] ADD 
CONSTRAINT [FK__DELETEQUEUEITEM_DELETEQUEUE] FOREIGN KEY 
(
	[ParentRowId]
) REFERENCES [dbo].[DeleteQueue] (
	[RowId]
)
GO

--FOREIGN KEY RELATIONSHIP [RepositoryActionType] -> [RepositoryStat] ([RepositoryActionType].[RepositoryActionTypeId] -> [RepositoryStat].[RepositoryActionTypeId])
if not exists(select * from sys.objects where name = 'FK__REPOSITORYSTAT_REPOSITORYACTIONTYPE' and type = 'F')
ALTER TABLE [dbo].[RepositoryStat] ADD 
CONSTRAINT [FK__REPOSITORYSTAT_REPOSITORYACTIONTYPE] FOREIGN KEY 
(
	[RepositoryActionTypeId]
) REFERENCES [dbo].[RepositoryActionType] (
	[RepositoryActionTypeId]
)
GO

--FOREIGN KEY RELATIONSHIP [Server] -> [ServerStat] ([Server].[ServerId] -> [ServerStat].[ServerId])
if not exists(select * from sys.objects where name = 'FK__SERVERSTAT_SERVER' and type = 'F')
ALTER TABLE [dbo].[ServerStat] ADD 
CONSTRAINT [FK__SERVERSTAT_SERVER] FOREIGN KEY 
(
	[ServerId]
) REFERENCES [dbo].[Server] (
	[ServerId]
)
GO

--##SECTION END [RELATIONS]

