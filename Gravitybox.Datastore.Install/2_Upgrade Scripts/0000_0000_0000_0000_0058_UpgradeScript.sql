--Generated Upgrade For Version 0.0.0.0.58
--Generated on 2017-10-26 14:49:50

--CREATE TABLE [CacheInvalidate]
if not exists(select * from sysobjects where name = 'CacheInvalidate' and xtype = 'U')
CREATE TABLE [dbo].[CacheInvalidate] (
	[RowId] [BigInt] IDENTITY (1, 1) NOT NULL ,
	[RepositoryId] [Int] NOT NULL ,
	[AddedDate] [DateTime2] (2) NOT NULL CONSTRAINT [DF__CACHEINVALIDATE_ADDEDDATE] DEFAULT (GetDate()),
	[Count] [Int] NOT NULL CONSTRAINT [DF__CACHEINVALIDATE_COUNT] DEFAULT (0),
	CONSTRAINT [PK_CACHEINVALIDATE] PRIMARY KEY CLUSTERED
	(
		[RowId]
	)
)

GO
--PRIMARY KEY FOR TABLE [CacheInvalidate]
if not exists(select * from sysobjects where name = 'PK_CACHEINVALIDATE' and xtype = 'PK')
ALTER TABLE [dbo].[CacheInvalidate] WITH NOCHECK ADD 
CONSTRAINT [PK_CACHEINVALIDATE] PRIMARY KEY CLUSTERED
(
	[RowId]
)

GO
--INDEX FOR TABLE [CacheInvalidate] COLUMNS:[RepositoryId]
if not exists(select * from sys.indexes where name = 'IDX_CACHEINVALIDATE_REPOSITORYID') and exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = 'RepositoryId' and o.name = 'CacheInvalidate')
CREATE NONCLUSTERED INDEX [IDX_CACHEINVALIDATE_REPOSITORYID] ON [dbo].[CacheInvalidate] ([RepositoryId] ASC)
GO

--INDEX FOR TABLE [CacheInvalidate] COLUMNS:[AddedDate]
if not exists(select * from sys.indexes where name = 'IDX_CACHEINVALIDATE_ADDEDDATE') and exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = 'AddedDate' and o.name = 'CacheInvalidate')
CREATE NONCLUSTERED INDEX [IDX_CACHEINVALIDATE_ADDEDDATE] ON [dbo].[CacheInvalidate] ([AddedDate] ASC)
GO

