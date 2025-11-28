# Files Needed for Render.com Deployment

## ✅ REQUIRED Files (Push to GitHub)

These are the **ONLY** files you need to push to GitHub for deployment:

### Core Application Files
- `app.py` - Main Flask application (95 KB)
- `requirements.txt` - Python dependencies (updated with gunicorn)
- `templates/` - HTML templates directory
  - `templates/login.html`
  - `templates/excel_upload.html`

### Deployment Configuration (to be created)
- `render.yaml` - Render.com service configuration
- `gunicorn_config.py` - Production WSGI server config
- `.env.example` - Environment variables template (for documentation)

### Documentation
- `README.md` - Project documentation (if exists)
- `DEPLOYMENT.md` - Deployment instructions (to be created)
- `.gitignore` - Git ignore rules

---

## ❌ EXCLUDED Files (Do NOT push to GitHub)

These directories are **automatically excluded** by `.gitignore` and should NOT be pushed:

| Directory | Size | Why Excluded |
|-----------|------|--------------|
| `.venv/` | 171 MB | Virtual environment - Render rebuilds this from `requirements.txt` |
| `build/` | 72 MB | PyInstaller build artifacts - only for local .exe creation |
| `dist/` | 47 MB | PyInstaller distribution - only for local .exe creation |
| `uploads/` | 24 MB | User uploaded files - runtime data only |
| `flask_session/` | <1 MB | Session data - runtime data only |
| `reports/` | <1 MB | Generated reports - runtime data only |
| `.env` | <1 KB | Contains secrets - configure in Render dashboard instead |

---

## 📦 Total GitHub Repository Size

**Approximately 100-150 KB** (just the source code and templates!)

---

## 🚀 Deployment Process

1. **Clean up** (optional but recommended):
   ```bash
   git rm -r --cached .venv build dist uploads flask_session reports
   ```

2. **Add only necessary files**:
   ```bash
   git add app.py requirements.txt templates/ render.yaml gunicorn_config.py .gitignore
   ```

3. **Commit and push**:
   ```bash
   git commit -m "Prepare for Render.com deployment"
   git push origin main
   ```

4. **On Render.com**:
   - Connect your GitHub repository
   - Render will automatically install dependencies from `requirements.txt`
   - Configure environment variables in the Render dashboard
   - Deploy!

---

## 📝 Notes

- **PyInstaller files** (`*.spec`, `build/`, `dist/`) are only needed for creating standalone Windows executables locally. They are NOT needed for web deployment.
- **Virtual environment** (`.venv/`) is recreated by Render during deployment.
- **Runtime directories** (`uploads/`, `flask_session/`, `reports/`) will be created automatically by the application on Render.
- **Environment variables** should be configured in Render's dashboard, not in `.env` file.
