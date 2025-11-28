# Render Deployment Debugging

## De Fout
`SyntaxError: Unexpected token '<', "<!doctype "... is not valid JSON`

Dit betekent dat je een HTML error pagina krijgt in plaats van je applicatie.

## Mogelijke Oorzaken & Oplossingen

### 1. ✅ Python Versie Fix (GEDAAN)
Python 3.11 kan problemen geven op Render. Ik heb het aangepast naar 3.10.12.

**Deploy opnieuw met:**
```bash
git add runtime.txt .python-version
git commit -m "Fix Python version for Render"
git push origin main
```

### 2. Check Render Build Logs
Ga naar Render Dashboard → je service → "Logs" tab

**Zoek naar:**
- `ERROR` - Build errors
- `ModuleNotFoundError` - Missing dependencies
- `Port` - Port binding issues

### 3. Manuele Setup (Als Blueprint faalt)
Probeer **ZONDER** `render.yaml`:

1. Verwijder `render.yaml` of negeer het
2. Maak service **manually** in Render:
   - Build Command: `pip install -r requirements.txt`
   - Start Command: `gunicorn --bind 0.0.0.0:$PORT app:app`
   - Add environment variable: `APP_SECRET` (Generate)

### 4. Test Lokaal Met Gunicorn
Voordat je deploy, test lokaal:
```bash
pip install gunicorn
gunicorn --bind 0.0.0.0:5008 app:app
```

Open: http://localhost:5008

Als dit NIET werkt lokaal, dan werkt het ook niet op Render!

---

## Next Steps

1. Herstel `app.py` met Ctrl+Z of `git checkout app.py`
2. Push Python version changes
3. Deploy opnieuw
4. Share de Render logs als het nog faalt
