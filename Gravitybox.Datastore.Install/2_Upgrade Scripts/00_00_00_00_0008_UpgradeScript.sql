--Generated Upgrade For Version 0.0.0.0.8
--Generated on 2014-06-07 22:22:21

--ADD COLUMN [DimensionValue].[DVIdx]
if exists(select * from sys.objects where name = 'DimensionValue' and type = 'U') AND not exists (select * from syscolumns c inner join sysobjects o on c.id = o.id where c.name = 'DVIdx' and o.name = 'DimensionValue')
ALTER TABLE [dbo].[DimensionValue] ADD [DVIdx] [BigInt] NOT NULL 

GO

--INDEX FOR TABLE [DimensionValue] COLUMNS:[DVIdx]
if not exists(select * from sys.indexes where name = 'IDX_DIMENSIONVALUE_DVIDX')
CREATE NONCLUSTERED INDEX [IDX_DIMENSIONVALUE_DVIDX] ON [dbo].[DimensionValue] ([DVIdx] ASC)

GO

--INDEX FOR TABLE [DimensionValue] COLUMNS:[Value]
if not exists(select * from sys.indexes where name = 'IDX_DIMENSIONVALUE_VALUE')
CREATE NONCLUSTERED INDEX [IDX_DIMENSIONVALUE_VALUE] ON [dbo].[DimensionValue] ([Value] ASC)

GO

