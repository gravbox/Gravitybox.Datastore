if not exists(select * from UserAccount)
insert into [UserAccount] (UniqueKey, UserName, [Password]) values ('00000000-0000-0000-0000-000000000000', 'root', 'password')
GO

--##METHODCALL [Gravitybox.Datastore.Install.CustomMethods.BuildDatastoreStopList]
GO