# Deployment Guide
## PE Portfolio Value-Creation Lakehouse — Executive Dashboard on Vercel

---

## Architecture

```
GitHub (main branch)
    │
    ├─── Push ──▶ GitHub Actions: dataops-quality-gate
    │                   │  Generate data + 35 PyTests + Full pipeline + Artefact checks
    │                   │  (fails fast — no deploy on broken tests)
    │                   ▼
    │             deploy-vercel job
    │                   │  Deploys web/ to Vercel Production
    │                   ▼
    │             https://pe-portfolio-fabric-lakehouse.vercel.app
    │                   │
    │                   └─▶ Health check (HTTP 200 assertion)
    │
    └─── Pull Request ──▶ dataops-quality-gate only (no deploy)
```

---

## One-Time Setup

### Step 1 — Create Vercel Account & Link Project

1. Go to [vercel.com](https://vercel.com) and sign in with your GitHub account.
2. Click **"Add New Project"** → Import `Devananditha/pe-portfolio-fabric-lakehouse`.
3. Vercel will auto-detect `vercel.json`; confirm these settings:
   - **Framework Preset**: Other
   - **Output Directory**: `web`
   - **Build Command**: *(leave blank)*
   - **Install Command**: *(leave blank)*
4. Click **Deploy** — this creates the initial production deployment.

### Step 2 — Retrieve Your Vercel Token

1. Go to [vercel.com/account/tokens](https://vercel.com/account/tokens).
2. Click **"Create Token"** → name it `github-actions-deploy` → set scope to **Full Account**.
3. Copy the token value immediately (shown only once).

### Step 3 — Add GitHub Secrets

In your GitHub repository:  
`Settings` → `Secrets and variables` → `Actions` → `New repository secret`

| Secret Name | Value |
| :--- | :--- |
| `VERCEL_TOKEN` | Your Vercel API token from Step 2 |

> **Note**: `VERCEL_ORG_ID` and `VERCEL_PROJECT_ID` are pulled automatically by `vercel pull` using your token.

### Step 4 — Verify End-to-End

Push any change to `main`. The Actions tab will show:

```
✅ Financial DataOps Quality Gate    — ~3m 30s
✅ Deploy Executive Dashboard        — ~45s
```

The live URL will be printed in the deploy step logs.

---

## What Gets Deployed

Vercel serves the `web/` directory as a static site:

| File | Size | Description |
| :--- | :--- | :--- |
| `web/index.html` | ~50 KB | Full executive dashboard (HTML + inline CSS + JS) |
| `web/data.js` | ~318 KB | Pre-compiled Gold mart payload (`window.PORTFOLIO_DATA`) |

**No server, no database, no backend** — the simulation engine runs entirely in the browser from the pre-compiled `data.js` payload.

### Response Headers (configured in `vercel.json`)

| Header | Value | Reason |
| :--- | :--- | :--- |
| `Content-Type` (data.js) | `application/javascript; charset=utf-8` | Correct MIME for module loading |
| `Cache-Control` (data.js) | `public, max-age=3600, stale-while-revalidate=86400` | 1h cache, 24h stale-while-revalidate |
| `X-Content-Type-Options` | `nosniff` | Security — prevents MIME sniffing |
| `X-Frame-Options` | `DENY` | Security — prevents clickjacking |
| `X-XSS-Protection` | `1; mode=block` | Legacy XSS filter |
| `Referrer-Policy` | `strict-origin-when-cross-origin` | Privacy |

---

## Refreshing the Dashboard Data

The `web/data.js` payload is pre-compiled from the Gold mart. To refresh it with new pipeline output:

```powershell
# 1. Regenerate and run the full pipeline
python run_pipeline.py --all

# 2. Recompile the web payload from the latest Gold Parquet
python scripts/export_web_data.py

# 3. Commit and push — CI auto-deploys on green tests
git add web/data.js
git commit -m "data: refresh web payload from Gold mart [YYYY-MM]"
git push origin main
```

---

## Local Preview

```powershell
# Option A — Python built-in server (simplest)
cd web
python -m http.server 8080
# Open: http://localhost:8080

# Option B — Node live-server (auto-reload)
npx live-server web --port=8080
```

---

## Troubleshooting

| Symptom | Likely Cause | Fix |
| :--- | :--- | :--- |
| CI deploy step fails with 401 | `VERCEL_TOKEN` secret not set or expired | Regenerate token and update GitHub secret |
| Dashboard loads but charts empty | `data.js` missing or 0-byte | Run `python scripts/export_web_data.py` and commit |
| Health check fails (non-200) | Vercel propagation delay | Increase `sleep 10` to `sleep 20` in CI workflow |
| `vercel.json` not detected | File in wrong location | Must be in repo root, not inside `web/` |
