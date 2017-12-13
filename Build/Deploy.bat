pause

net stop "Gravitybox Datastore"

RD /S /Q "C:\Deploy-DS\temp"

rem UNZIP THE DEPLOY FILE
"C:\Program Files\7-Zip\7z.exe" x -o"C:\Deploy-DS\temp" "C:\Deploy-DS\gravity.datastore.zip"

rem COPY SERVICES CACHE TO DEPLOY FOLDERS
mkdir C:\Services\Datastore\
xcopy /Y /s /i "C:\Deploy-DS\temp\*" "C:\Services\Datastore\"

rem START SERVICES
net start "Gravitybox Datastore"

pause