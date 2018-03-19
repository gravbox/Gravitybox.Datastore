--Generated Upgrade For Version 0.0.0.0.3
--Generated on 2014-10-29 13:03:27

--CREATE TABLE [AppliedPatch]
if not exists(select * from sysobjects where name = 'AppliedPatch' and xtype = 'U')
CREATE TABLE [dbo].[AppliedPatch] (
	[ID] [UniqueIdentifier] NOT NULL CONSTRAINT [DF__APPLIEDPATCH_ID] DEFAULT (newid()),
	[Description] [VarChar] (50) NULL ,
	[ModifiedBy] [Varchar] (50) NULL,
	[ModifiedDate] [DateTime] CONSTRAINT [DF__APPLIEDPATCH_MODIFIEDDATE] DEFAULT sysdatetime() NULL,
	[CreatedBy] [Varchar] (50) NULL,
	[CreatedDate] [DateTime] CONSTRAINT [DF__APPLIEDPATCH_CREATEDDATE] DEFAULT sysdatetime() NULL,
	[Timestamp] [ROWVERSION] NOT NULL
)

GO
--PRIMARY KEY FOR TABLE [AppliedPatch]
if not exists(select * from sysobjects where name = 'PK_APPLIEDPATCH' and xtype = 'PK')
ALTER TABLE [dbo].[AppliedPatch] WITH NOCHECK ADD 
CONSTRAINT [PK_APPLIEDPATCH] PRIMARY KEY CLUSTERED
(
	[ID]
)

GO
