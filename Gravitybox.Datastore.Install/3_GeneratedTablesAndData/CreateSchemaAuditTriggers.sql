--DO NOT MODIFY THIS FILE. IT IS ALWAYS OVERWRITTEN ON GENERATION.
--Audit Triggers

--##SECTION BEGIN [AUDIT TRIGGERS]

--DROP ANY AUDIT TRIGGERS FOR [dbo].[AppliedPatch]
if exists(select * from sysobjects where name = '__TR_AppliedPatch__INSERT' AND xtype = 'TR')
DROP TRIGGER [dbo].[__TR_AppliedPatch__INSERT]
GO
if exists(select * from sysobjects where name = '__TR_AppliedPatch__UPDATE' AND xtype = 'TR')
DROP TRIGGER [dbo].[__TR_AppliedPatch__UPDATE]
GO
if exists(select * from sysobjects where name = '__TR_AppliedPatch__DELETE' AND xtype = 'TR')
DROP TRIGGER [dbo].[__TR_AppliedPatch__DELETE]
GO

--DROP ANY AUDIT TRIGGERS FOR [dbo].[CacheInvalidate]
if exists(select * from sysobjects where name = '__TR_CacheInvalidate__INSERT' AND xtype = 'TR')
DROP TRIGGER [dbo].[__TR_CacheInvalidate__INSERT]
GO
if exists(select * from sysobjects where name = '__TR_CacheInvalidate__UPDATE' AND xtype = 'TR')
DROP TRIGGER [dbo].[__TR_CacheInvalidate__UPDATE]
GO
if exists(select * from sysobjects where name = '__TR_CacheInvalidate__DELETE' AND xtype = 'TR')
DROP TRIGGER [dbo].[__TR_CacheInvalidate__DELETE]
GO

--DROP ANY AUDIT TRIGGERS FOR [dbo].[ConfigurationSetting]
if exists(select * from sysobjects where name = '__TR_ConfigurationSetting__INSERT' AND xtype = 'TR')
DROP TRIGGER [dbo].[__TR_ConfigurationSetting__INSERT]
GO
if exists(select * from sysobjects where name = '__TR_ConfigurationSetting__UPDATE' AND xtype = 'TR')
DROP TRIGGER [dbo].[__TR_ConfigurationSetting__UPDATE]
GO
if exists(select * from sysobjects where name = '__TR_ConfigurationSetting__DELETE' AND xtype = 'TR')
DROP TRIGGER [dbo].[__TR_ConfigurationSetting__DELETE]
GO

--DROP ANY AUDIT TRIGGERS FOR [dbo].[Housekeeping]
if exists(select * from sysobjects where name = '__TR_Housekeeping__INSERT' AND xtype = 'TR')
DROP TRIGGER [dbo].[__TR_Housekeeping__INSERT]
GO
if exists(select * from sysobjects where name = '__TR_Housekeeping__UPDATE' AND xtype = 'TR')
DROP TRIGGER [dbo].[__TR_Housekeeping__UPDATE]
GO
if exists(select * from sysobjects where name = '__TR_Housekeeping__DELETE' AND xtype = 'TR')
DROP TRIGGER [dbo].[__TR_Housekeeping__DELETE]
GO

--DROP ANY AUDIT TRIGGERS FOR [dbo].[Lock]
if exists(select * from sysobjects where name = '__TR_Lock__INSERT' AND xtype = 'TR')
DROP TRIGGER [dbo].[__TR_Lock__INSERT]
GO
if exists(select * from sysobjects where name = '__TR_Lock__UPDATE' AND xtype = 'TR')
DROP TRIGGER [dbo].[__TR_Lock__UPDATE]
GO
if exists(select * from sysobjects where name = '__TR_Lock__DELETE' AND xtype = 'TR')
DROP TRIGGER [dbo].[__TR_Lock__DELETE]
GO

--DROP ANY AUDIT TRIGGERS FOR [dbo].[LockStat]
if exists(select * from sysobjects where name = '__TR_LockStat__INSERT' AND xtype = 'TR')
DROP TRIGGER [dbo].[__TR_LockStat__INSERT]
GO
if exists(select * from sysobjects where name = '__TR_LockStat__UPDATE' AND xtype = 'TR')
DROP TRIGGER [dbo].[__TR_LockStat__UPDATE]
GO
if exists(select * from sysobjects where name = '__TR_LockStat__DELETE' AND xtype = 'TR')
DROP TRIGGER [dbo].[__TR_LockStat__DELETE]
GO

--DROP ANY AUDIT TRIGGERS FOR [dbo].[Machine]
if exists(select * from sysobjects where name = '__TR_Machine__INSERT' AND xtype = 'TR')
DROP TRIGGER [dbo].[__TR_Machine__INSERT]
GO
if exists(select * from sysobjects where name = '__TR_Machine__UPDATE' AND xtype = 'TR')
DROP TRIGGER [dbo].[__TR_Machine__UPDATE]
GO
if exists(select * from sysobjects where name = '__TR_Machine__DELETE' AND xtype = 'TR')
DROP TRIGGER [dbo].[__TR_Machine__DELETE]
GO

--DROP ANY AUDIT TRIGGERS FOR [dbo].[Repository]
if exists(select * from sysobjects where name = '__TR_Repository__INSERT' AND xtype = 'TR')
DROP TRIGGER [dbo].[__TR_Repository__INSERT]
GO
if exists(select * from sysobjects where name = '__TR_Repository__UPDATE' AND xtype = 'TR')
DROP TRIGGER [dbo].[__TR_Repository__UPDATE]
GO
if exists(select * from sysobjects where name = '__TR_Repository__DELETE' AND xtype = 'TR')
DROP TRIGGER [dbo].[__TR_Repository__DELETE]
GO

--DROP ANY AUDIT TRIGGERS FOR [dbo].[RepositoryActionType]
if exists(select * from sysobjects where name = '__TR_RepositoryActionType__INSERT' AND xtype = 'TR')
DROP TRIGGER [dbo].[__TR_RepositoryActionType__INSERT]
GO
if exists(select * from sysobjects where name = '__TR_RepositoryActionType__UPDATE' AND xtype = 'TR')
DROP TRIGGER [dbo].[__TR_RepositoryActionType__UPDATE]
GO
if exists(select * from sysobjects where name = '__TR_RepositoryActionType__DELETE' AND xtype = 'TR')
DROP TRIGGER [dbo].[__TR_RepositoryActionType__DELETE]
GO

--DROP ANY AUDIT TRIGGERS FOR [dbo].[RepositoryLog]
if exists(select * from sysobjects where name = '__TR_RepositoryLog__INSERT' AND xtype = 'TR')
DROP TRIGGER [dbo].[__TR_RepositoryLog__INSERT]
GO
if exists(select * from sysobjects where name = '__TR_RepositoryLog__UPDATE' AND xtype = 'TR')
DROP TRIGGER [dbo].[__TR_RepositoryLog__UPDATE]
GO
if exists(select * from sysobjects where name = '__TR_RepositoryLog__DELETE' AND xtype = 'TR')
DROP TRIGGER [dbo].[__TR_RepositoryLog__DELETE]
GO

--DROP ANY AUDIT TRIGGERS FOR [dbo].[RepositoryStat]
if exists(select * from sysobjects where name = '__TR_RepositoryStat__INSERT' AND xtype = 'TR')
DROP TRIGGER [dbo].[__TR_RepositoryStat__INSERT]
GO
if exists(select * from sysobjects where name = '__TR_RepositoryStat__UPDATE' AND xtype = 'TR')
DROP TRIGGER [dbo].[__TR_RepositoryStat__UPDATE]
GO
if exists(select * from sysobjects where name = '__TR_RepositoryStat__DELETE' AND xtype = 'TR')
DROP TRIGGER [dbo].[__TR_RepositoryStat__DELETE]
GO

--DROP ANY AUDIT TRIGGERS FOR [dbo].[Server]
if exists(select * from sysobjects where name = '__TR_Server__INSERT' AND xtype = 'TR')
DROP TRIGGER [dbo].[__TR_Server__INSERT]
GO
if exists(select * from sysobjects where name = '__TR_Server__UPDATE' AND xtype = 'TR')
DROP TRIGGER [dbo].[__TR_Server__UPDATE]
GO
if exists(select * from sysobjects where name = '__TR_Server__DELETE' AND xtype = 'TR')
DROP TRIGGER [dbo].[__TR_Server__DELETE]
GO

--DROP ANY AUDIT TRIGGERS FOR [dbo].[ServerStat]
if exists(select * from sysobjects where name = '__TR_ServerStat__INSERT' AND xtype = 'TR')
DROP TRIGGER [dbo].[__TR_ServerStat__INSERT]
GO
if exists(select * from sysobjects where name = '__TR_ServerStat__UPDATE' AND xtype = 'TR')
DROP TRIGGER [dbo].[__TR_ServerStat__UPDATE]
GO
if exists(select * from sysobjects where name = '__TR_ServerStat__DELETE' AND xtype = 'TR')
DROP TRIGGER [dbo].[__TR_ServerStat__DELETE]
GO

--DROP ANY AUDIT TRIGGERS FOR [dbo].[UserAccount]
if exists(select * from sysobjects where name = '__TR_UserAccount__INSERT' AND xtype = 'TR')
DROP TRIGGER [dbo].[__TR_UserAccount__INSERT]
GO
if exists(select * from sysobjects where name = '__TR_UserAccount__UPDATE' AND xtype = 'TR')
DROP TRIGGER [dbo].[__TR_UserAccount__UPDATE]
GO
if exists(select * from sysobjects where name = '__TR_UserAccount__DELETE' AND xtype = 'TR')
DROP TRIGGER [dbo].[__TR_UserAccount__DELETE]
GO

--##SECTION END [AUDIT TRIGGERS]

