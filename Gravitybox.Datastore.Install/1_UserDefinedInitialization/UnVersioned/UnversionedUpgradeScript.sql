--Generated Unversioned Upgrade
--Generated on 2014-06-06 16:49:42

--UNCOMMENT TO DROP ALL DEFAULTS IF NEEDED. IF THIS MODEL WAS IMPORTED FROM AN EXISTSING DATABASE THE MODEL WILL RECREATE ALL DEFAULTS WITH A GENERATED NAME.
--DROP ALL DEFAULTS
--DECLARE @SqlCmd varchar(4000); SET @SqlCmd = ''
--DECLARE @Cnt int; SET @Cnt = 0
--select @Cnt = count(*) from sysobjects d
--join  sysobjects o on d.parent_obj = o.id
--where d.xtype = 'D'
 
--WHILE @Cnt > 0
--BEGIN
--      select TOP 1 @SqlCmd = 'ALTER TABLE ' + o.name + ' DROP CONSTRAINT ' + d.name
--      from sysobjects d
--      join sysobjects o on d.parent_obj = o.id
--      where d.xtype = 'D'
--      EXEC(@SqlCmd) --SELECT @SqlCmd --view the command only
--      select @Cnt = count(*) from   sysobjects d
--      join  sysobjects o on d.parent_obj = o.id
--      where d.xtype = 'D'
--END
--GO

--RENAME OLD INDEXES FROM THE IMPORT DATABASE IF NEEDED

