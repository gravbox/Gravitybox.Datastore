--Generated Upgrade For Version 0.0.0.0.16
--Generated on 2014-06-10 08:27:19

--ADD COLUMN [Repository].[IsDeleted]
if exists(select * from sys.objects where name = 'Repository' and type = 'U') AND not exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = 'IsDeleted' and o.name = 'Repository')
ALTER TABLE [dbo].[Repository] ADD [IsDeleted] [Bit] NOT NULL CONSTRAINT [DF__REPOSITORY_ISDELETED] DEFAULT (0)

GO

--ADD COLUMN [Repository].[IsInitialized]
if exists(select * from sys.objects where name = 'Repository' and type = 'U') AND not exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = 'IsInitialized' and o.name = 'Repository')
ALTER TABLE [dbo].[Repository] ADD [IsInitialized] [Bit] NOT NULL CONSTRAINT [DF__REPOSITORY_ISINITIALIZED] DEFAULT (1)

GO

--INDEX FOR TABLE [Repository] COLUMNS:[IsDeleted]
if not exists(select * from sys.indexes where name = 'IDX_REPOSITORY_ISDELETED')
CREATE NONCLUSTERED INDEX [IDX_REPOSITORY_ISDELETED] ON [dbo].[Repository] ([IsDeleted] ASC)

GO

--INDEX FOR TABLE [Repository] COLUMNS:[IsInitialized]
if not exists(select * from sys.indexes where name = 'IDX_REPOSITORY_ISINITIALIZED')
CREATE NONCLUSTERED INDEX [IDX_REPOSITORY_ISINITIALIZED] ON [dbo].[Repository] ([IsInitialized] ASC)

GO

