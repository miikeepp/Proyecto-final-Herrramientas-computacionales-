@echo off
rem Abre la presentacion con un servidor local para que carguen los modelos 3D (.glb).
rem Se sirve desde la raiz del proyecto para que tambien carguen las imagenes de data/.
cd /d "%~dp0.."
start "Servidor de la presentacion (cierra esta ventana para detenerlo)" /min python -m http.server 8000
timeout /t 2 /nobreak >nul
start "" "http://localhost:8000/Presentacion%%20Herramientas%%20computacionales/index.html"
