--Generated Upgrade For Version 0.0.0.0.38
--Generated on 2015-04-30 10:18:55

--CREATE TABLE [LockStat]
if not exists(select * from sysobjects where name = 'LockStat' and xtype = 'U')
CREATE TABLE [dbo].[LockStat] (
	[LockStatId] [Int] IDENTITY (1, 1) NOT NULL ,
	[ThreadId] [Int] NOT NULL ,
	[Failure] [Bit] NOT NULL ,
	[Elapsed] [Int] NOT NULL ,
	[CurrentReadCount] [Int] NOT NULL ,
	[WaitingReadCount] [Int] NOT NULL ,
	[WaitingWriteCount] [Int] NOT NULL ,
	[IsWriteLockHeld] [Bit] NOT NULL ,
	[CreatedBy] [Varchar] (50) NULL,
	[CreatedDate] [DateTime] CONSTRAINT [DF__LOCKSTAT_CREATEDDATE] DEFAULT sysdatetime() NULL)

GO
--PRIMARY KEY FOR TABLE [LockStat]
if not exists(select * from sysobjects where name = 'PK_LOCKSTAT' and xtype = 'PK')
ALTER TABLE [dbo].[LockStat] WITH NOCHECK ADD 
CONSTRAINT [PK_LOCKSTAT] PRIMARY KEY CLUSTERED
(
	[LockStatId]
)

GO
