--DO NOT MODIFY THIS FILE. IT IS ALWAYS OVERWRITTEN ON GENERATION.
--Relations

--##SECTION BEGIN [RELATIONS]

--FOREIGN KEY RELATIONSHIP [RepositoryActionType] -> [RepositoryStat] ([RepositoryActionType].[RepositoryActionTypeId] -> [RepositoryStat].[RepositoryActionTypeId])
if not exists(select * from sysobjects where name = 'FK__REPOSITORYSTAT_REPOSITORYACTIONTYPE' and xtype = 'F')
ALTER TABLE [dbo].[RepositoryStat] ADD 
CONSTRAINT [FK__REPOSITORYSTAT_REPOSITORYACTIONTYPE] FOREIGN KEY 
(
	[RepositoryActionTypeId]
) REFERENCES [dbo].[RepositoryActionType] (
	[RepositoryActionTypeId]
)
GO

--FOREIGN KEY RELATIONSHIP [Server] -> [ServerStat] ([Server].[ServerId] -> [ServerStat].[ServerId])
if not exists(select * from sysobjects where name = 'FK__SERVERSTAT_SERVER' and xtype = 'F')
ALTER TABLE [dbo].[ServerStat] ADD 
CONSTRAINT [FK__SERVERSTAT_SERVER] FOREIGN KEY 
(
	[ServerId]
) REFERENCES [dbo].[Server] (
	[ServerId]
)
GO

--##SECTION END [RELATIONS]

