@echo off
set NETWORK_PORT=5000
set NETWORK_HOST=localhost

echo -------------------------------
echo Starting Network Controller...
echo -------------------------------
start cmd /k python main.py --network --host 0.0.0.0 --network-port %NETWORK_PORT%
timeout /T 2 >nul

echo -------------------------------
echo Starting Node 1...
echo -------------------------------
start cmd /k python main.py --node --node-id node1 --cpu 4 --memory 16 --storage 500 --bandwidth 1000 --network-host %NETWORK_HOST% --network-port %NETWORK_PORT%

echo -------------------------------
echo Starting Node 2...
echo -------------------------------
start cmd /k python main.py --node --node-id node2 --cpu 4 --memory 16 --storage 500 --bandwidth 1000 --network-host %NETWORK_HOST% --network-port %NETWORK_PORT%

echo -------------------------------
echo Starting Node 3...
echo -------------------------------
start cmd /k python main.py --node --node-id node3 --cpu 4 --memory 16 --storage 500 --bandwidth 1000 --network-host %NETWORK_HOST% --network-port %NETWORK_PORT%

echo.
echo Cluster launched successfully !
echo Network + 3 nodes running in separate CMD windows.
echo.
pause
