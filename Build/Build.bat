RD /S /Q "C:\publish\Datastore"
mkdir "C:\publish\Datastore"

"C:\Program Files (x86)\Microsoft Visual Studio\2017\BuildTools\MSBuild\15.0\Bin\msbuild.exe" "..\Gravitybox.Datastore.sln" /p:Configuration=Release /t:Rebuild

rem COPY SERVICE
copy "C:\Projects\Gravitybox.Datastore\Gravitybox.Datastore.WinService\bin\Release\*.exe" "C:\publish\Datastore\"
copy "C:\Projects\Gravitybox.Datastore\Gravitybox.Datastore.WinService\bin\Release\*.dll" "C:\publish\Datastore\"
copy "C:\Projects\Gravitybox.Datastore\Gravitybox.Datastore.WinService\bin\Release\*.pdb" "C:\publish\Datastore\"
copy "C:\Projects\Gravitybox.Datastore\Gravitybox.Datastore.WinService\bin\Release\NLog.config" "C:\publish\Datastore\"
copy "C:\Projects\Gravitybox.Datastore\Gravitybox.Datastore.WinService\bin\Release\Gravitybox.Datastore.WinService.exe.config" "C:\publish\Datastore\Gravitybox.Datastore.WinService.exe.config.source"

rem ZIP THE FOLDERS
del /q "C:\publish\gravity.datastore.zip"
"C:\Program Files\7-Zip\7z.exe" a "C:\publish\gravity.datastore.zip" "C:\publish\Datastore\*.*"

pause