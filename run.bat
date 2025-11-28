@echo off
setlocal

REM Create venv if missing
if not exist ".venv\" (
  py -3 -m venv .venv
)

call .venv\Scripts\activate

pip install --upgrade pip
pip install -r requirements.txt

set FLASK_SECRET_KEY=changeme-please
set PORT=5003

python app.py

endlocal
