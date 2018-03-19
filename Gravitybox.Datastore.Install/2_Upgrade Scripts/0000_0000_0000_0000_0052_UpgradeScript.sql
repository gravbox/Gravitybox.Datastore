--Generated Upgrade For Version 0.0.0.0.52
--Generated on 2017-03-09 11:31:09

--CREATE TABLE [Housekeeping]
if not exists(select * from sysobjects where name = 'Housekeeping' and xtype = 'U')
CREATE TABLE [dbo].[Housekeeping] (
	[ID] [Int] IDENTITY (1, 1) NOT NULL ,
	[Data] [VarBinary] (max) NOT NULL ,
	[Type] [Int] NOT NULL ,
	[Timestamp] [ROWVERSION] NOT NULL,
	CONSTRAINT [PK_HOUSEKEEPING] PRIMARY KEY CLUSTERED
	(
		[ID]
	)
)

GO
--PRIMARY KEY FOR TABLE [Housekeeping]
if not exists(select * from sysobjects where name = 'PK_HOUSEKEEPING' and xtype = 'PK')
ALTER TABLE [dbo].[Housekeeping] WITH NOCHECK ADD 
CONSTRAINT [PK_HOUSEKEEPING] PRIMARY KEY CLUSTERED
(
	[ID]
)

GO
