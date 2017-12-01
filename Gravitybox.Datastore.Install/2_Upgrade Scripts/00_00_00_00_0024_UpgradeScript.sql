--Generated Upgrade For Version 0.0.0.0.24
--Generated on 2014-06-13 15:34:51

--CREATE TABLE [Lock]
if not exists(select * from sysobjects where name = 'Lock' and xtype = 'U')
CREATE TABLE [dbo].[Lock] (
	[LockId] [Int] IDENTITY (1, 1) NOT NULL ,
	[MachineId] [Int] NOT NULL ,
	[LockTime] [DateTime] NOT NULL ,
	[IsRead] [Bit] NOT NULL ,
	[Intention] [Bit] NOT NULL )

GO
--PRIMARY KEY FOR TABLE [Lock]
if not exists(select * from sysobjects where name = 'PK_LOCK' and xtype = 'PK')
ALTER TABLE [dbo].[Lock] WITH NOCHECK ADD 
CONSTRAINT [PK_LOCK] PRIMARY KEY CLUSTERED
(
	[LockId]
)

GO
--CREATE TABLE [Machine]
if not exists(select * from sysobjects where name = 'Machine' and xtype = 'U')
CREATE TABLE [dbo].[Machine] (
	[MachineId] [Int] IDENTITY (1, 1) NOT NULL ,
	[Name] [NVarChar] (100) NOT NULL )

GO
--PRIMARY KEY FOR TABLE [Machine]
if not exists(select * from sysobjects where name = 'PK_MACHINE' and xtype = 'PK')
ALTER TABLE [dbo].[Machine] WITH NOCHECK ADD 
CONSTRAINT [PK_MACHINE] PRIMARY KEY CLUSTERED
(
	[MachineId]
)

GO
