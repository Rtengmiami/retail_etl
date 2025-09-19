@echo off
REM Online Retail II ETL Pipeline - Standalone Deployment
REM 獨立版本啟動腳本 (Windows)

echo 啟動 Online Retail II ETL Pipeline (獨立版本)...

REM 設定必要的環境變數
set AIRFLOW_UID=50000
for %%i in ("%cd%") do set AIRFLOW_PROJ_DIR=%%~fi
for %%i in ("%cd%") do set PROJECT_ROOT=%%~fi

echo.
echo 環境設定:
echo    AIRFLOW_UID: %AIRFLOW_UID%
echo    AIRFLOW_PROJ_DIR: %AIRFLOW_PROJ_DIR%
echo    PROJECT_ROOT: %PROJECT_ROOT%
echo    當前目錄: %cd%

REM 建立必要目錄
if not exist "logs" mkdir logs
if not exist "data\raw" mkdir data\raw
if not exist "data\processed" mkdir data\processed
if not exist "data\quality_reports" mkdir data\quality_reports

echo.
echo 檢查關鍵檔案...
if exist "docker-compose-simple.yaml" (
    echo    [OK] Docker Compose 檔案存在
) else (
    echo    [ERROR] Docker Compose 檔案不存在
    pause
    exit /b 1
)

if exist ".env" (
    echo    [OK] 環境變數檔案存在
) else (
    echo    [ERROR] 環境變數檔案不存在
    pause
    exit /b 1
)

if exist "data\raw\online_retail_II.xlsx" (
    echo    [OK] 數據文件存在
) else (
    echo    [WARNING] 數據文件不存在: data\raw\online_retail_II.xlsx
    echo             請確認已下載 online_retail_II.xlsx 到 data\raw\ 目錄
)

echo.
echo 啟動 Airflow 服務...

REM 啟動服務
docker-compose -f docker-compose-simple.yaml up -d

echo.
echo 等待服務啟動...
timeout /t 10 /nobreak > nul

REM 檢查服務狀態
echo 服務狀態:
docker-compose -f docker-compose-simple.yaml ps

echo.
echo 啟動完成！
echo.
echo Airflow UI: http://localhost:8080
echo 預設帳號: airflow / airflow
echo.
echo 資料庫連線:
echo    PostgreSQL (Retail DW): localhost:5433
echo    - 資料庫: retail_dw
echo    - 用戶: airflow / airflow
echo.
echo 查看日誌：
echo    docker-compose logs -f [service_name]
echo.
echo 停止服務：
echo    stop_standalone.bat
echo.
pause