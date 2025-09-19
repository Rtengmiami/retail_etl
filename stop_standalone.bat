@echo off
REM Online Retail II ETL Pipeline - Standalone Deployment
REM 獨立版本停止腳本 (Windows)

echo 停止 Online Retail II ETL Pipeline (獨立版本)...

REM 停止服務
docker-compose -f docker-compose-simple.yaml down

echo.
echo 服務已停止
echo.
echo 如需清理資料卷 (會刪除所有資料):
echo    docker-compose -f docker-compose-simple.yaml down -v
echo.
pause