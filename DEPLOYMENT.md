# Deploying to Render.com

This guide walks you through deploying the Odoo Excel Uploader application to Render.com.

## Prerequisites

- A [Render.com](https://render.com) account (free tier available)
- A Git repository (GitHub, GitLab, or Bitbucket) with your code
- Git installed locally

---

## Step 1: Prepare Your Repository

### 1.1 Initialize Git (if not already done)

```bash
cd c:\pyhton\Odooimportappfinal2025
git init
```

### 1.2 Add Files to Git

Only add the necessary files (see [DEPLOYMENT_FILES.md](file:///c:/pyhton/Odooimportappfinal2025/DEPLOYMENT_FILES.md) for details):

```bash
git add app.py requirements.txt templates/ render.yaml gunicorn_config.py .gitignore .env.example DEPLOYMENT.md DEPLOYMENT_FILES.md
git commit -m "Initial commit - ready for Render deployment"
```

### 1.3 Push to GitHub

Create a new repository on GitHub, then:

```bash
git remote add origin https://github.com/YOUR_USERNAME/YOUR_REPO_NAME.git
git branch -M main
git push -u origin main
```

> **Note**: Your repository will be ~100-150 KB (not 314 MB!) because `.gitignore` excludes `.venv/`, `build/`, `dist/`, and other large directories.

---

## Step 2: Create Web Service on Render

### 2.1 Connect Repository

1. Log in to [Render.com](https://dashboard.render.com)
2. Click **"New +"** → **"Web Service"**
3. Connect your GitHub/GitLab/Bitbucket account
4. Select your repository

### 2.2 Configure Service

Render will auto-detect the `render.yaml` file. Verify these settings:

| Setting | Value |
|---------|-------|
| **Name** | `odoo-excel-uploader` (or your choice) |
| **Runtime** | `Python 3` |
| **Build Command** | `pip install -r requirements.txt` |
| **Start Command** | `gunicorn --config gunicorn_config.py app:app` |
| **Plan** | `Free` (or paid plan for production) |

---

## Step 3: Configure Environment Variables

In the Render dashboard, add these environment variables:

### Required Variables

| Variable | Value | Description |
|----------|-------|-------------|
| `APP_SECRET` | `<random-string>` | Flask secret key (click "Generate" in Render) |

### Optional Variables (with defaults)

| Variable | Default | Description |
|----------|---------|-------------|
| `FAST_MODE` | `false` | Enable fast mode |
| `IMAGE_WORKERS` | `12` | Number of image processing workers |
| `MAX_IMG_PX` | `1024` | Maximum image pixel size |
| `JPEG_QUALITY` | `80` | JPEG compression quality |

### Odoo-Specific Variables

Add any Odoo connection credentials your application needs (these are not in the current code but may be needed):

```
ODOO_URL=https://your-odoo-instance.com
ODOO_DB=your-database-name
ODOO_USERNAME=your-username
ODOO_PASSWORD=your-password
```

---

## Step 4: Deploy

1. Click **"Create Web Service"**
2. Render will:
   - Clone your repository
   - Install dependencies from `requirements.txt`
   - Start the application with Gunicorn
3. Wait for deployment to complete (~2-5 minutes)
4. Your app will be available at: `https://your-app-name.onrender.com`

---

## Important Notes

### ⚠️ Ephemeral Storage

Render's free tier uses **ephemeral storage**, meaning:

- **`uploads/`** directory is cleared on each deployment
- **`flask_session/`** directory is cleared on each deployment
- **`reports/`** directory is cleared on each deployment

**For production use**, consider:
- Using **Redis** for session storage instead of filesystem
- Using **cloud storage** (AWS S3, Google Cloud Storage, Cloudflare R2) for file uploads
- Using a **database** to store reports

### 🔄 Auto-Deploy

The `render.yaml` configuration enables auto-deploy. Every time you push to your `main` branch, Render will automatically redeploy your application.

To disable auto-deploy:
1. Go to your service settings in Render dashboard
2. Disable "Auto-Deploy"

### 📊 Monitoring

- **Logs**: View real-time logs in the Render dashboard
- **Metrics**: Monitor CPU, memory, and bandwidth usage
- **Health Checks**: Render automatically checks your app's health at `/`

---

## Troubleshooting

### Build Fails

**Error**: `Could not find a version that satisfies the requirement...`

**Solution**: Check that all dependencies in `requirements.txt` are compatible with Python 3.11

### Application Won't Start

**Error**: `Failed to bind to $PORT`

**Solution**: Ensure `gunicorn_config.py` uses `os.environ.get('PORT', '10000')`

### Session Issues

**Error**: Sessions not persisting between requests

**Solution**: 
1. Verify `APP_SECRET` is set in environment variables
2. Consider switching to Redis-based sessions for production:
   ```python
   app.config["SESSION_TYPE"] = "redis"
   app.config["SESSION_REDIS"] = redis.from_url(os.environ.get("REDIS_URL"))
   ```

### File Upload Issues

**Error**: Uploaded files disappear after deployment

**Solution**: This is expected on Render's free tier (ephemeral storage). For production:
1. Use cloud storage (S3, GCS, etc.)
2. Upgrade to a paid Render plan with persistent disks

---

## Updating Your Application

1. Make changes locally
2. Commit and push to GitHub:
   ```bash
   git add .
   git commit -m "Your update message"
   git push origin main
   ```
3. Render will automatically redeploy (if auto-deploy is enabled)

---

## Cost

- **Free Tier**: 
  - 750 hours/month of runtime
  - Spins down after 15 minutes of inactivity
  - 512 MB RAM
  - Ephemeral storage

- **Paid Plans**: Starting at $7/month
  - Always-on service
  - More RAM and CPU
  - Optional persistent disks

---

## Additional Resources

- [Render Documentation](https://render.com/docs)
- [Render Python Guide](https://render.com/docs/deploy-flask)
- [Gunicorn Documentation](https://docs.gunicorn.org/)
