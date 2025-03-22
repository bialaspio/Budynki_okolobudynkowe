@echo off
set OSGEO4W_ROOT=D:\OSGeo4W
PATH=%OSGEO4W_ROOT%\bin;%PATH%
for %%f in (%OSGEO4W_ROOT%\etc\ini\*.bat) do call %%f
@echo on

set PGCLIENTENCODING=WIN1250
set host=127.0.0.1
set label=ID

for %%I in (*.shp) do (
ogr2ogr -f "PostgreSQL" "PG:dbname=krakowski_budynki_v02 user=postgres password=aaaaaa host=%host% port=5432" %%I -nln %%~nI -nlt GEOMETRY
)

pause