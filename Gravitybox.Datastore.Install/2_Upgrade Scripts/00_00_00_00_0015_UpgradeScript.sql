----Generated Upgrade For Version 0.0.0.0.15
----Generated on 2014-06-09 16:35:54

----DELETE INDEX
--if exists(select * from sys.indexes where name = 'IDX_DIMENSION_DIDX')
--DROP INDEX [IDX_DIMENSION_DIDX] ON [dbo].[Dimension]

--GO
----DELETE INDEX
--if exists(select * from sys.indexes where name = 'IDX_DIMENSION_REPOSITORYID')
--DROP INDEX [IDX_DIMENSION_REPOSITORYID] ON [dbo].[Dimension]

--GO
----DELETE INDEX
--if exists(select * from sys.indexes where name = 'IDX_DIMENSIONVALUE_DIMENSIONID')
--DROP INDEX [IDX_DIMENSIONVALUE_DIMENSIONID] ON [dbo].[DimensionValue]

--GO
----DELETE INDEX
--if exists(select * from sys.indexes where name = 'IDX_DIMENSIONVALUE_DVIDX')
--DROP INDEX [IDX_DIMENSIONVALUE_DVIDX] ON [dbo].[DimensionValue]

--GO
----DELETE INDEX
--if exists(select * from sys.indexes where name = 'IDX_DIMENSIONVALUE_VALUE')
--DROP INDEX [IDX_DIMENSIONVALUE_VALUE] ON [dbo].[DimensionValue]

--GO
