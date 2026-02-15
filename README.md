# Minerva Code – Factor portfolio pipeline

Factor pipeline: Value, Momentum, Liquidity, Quality, Yield (BETA0/BETA1/BETA2), Low Vol. Builds regional (US/EU) and combined factor returns and runs portfolio analysis.

## Setup

From the project root (folder containing `run_all.py` and `src/`):

```bash
pip install -r requirements.txt
```

## Run

- **Full pipeline (no notebooks):**  
  `python run_all.py`  
  Default: combined region. Use `--region us` or `--region eu` for regional outputs.

- **Notebooks (run pipeline + analysis + plots):**  
  Open and run all cells. Run from project root so imports work.
  - `portfolio_analysis.ipynb` – runs pipeline, scenario analysis, saves plots to `outputs/plots/`
  - `Factor_Portfolio_US.ipynb` – US factor matrix, metrics, distributions, heatmap → `outputs/plots/us/`
  - `Factor_Portfolio_EU.ipynb` – EU same → `outputs/plots/eu/`

## Project structure

Keep everything under one project root. Suggested layout:

```
Minerva Code/
├── .gitignore
├── README.md
├── requirements.txt
├── config.py              # Weights, active factors, paths
├── run_all.py             # Run all factors + build portfolio outputs
├── portfolio_analysis.ipynb
├── Factor_Portfolio_US.ipynb
├── Factor_Portfolio_EU.ipynb
├── factor_performance_backtest.ipynb
├── data/                  # Put input data here (ignored by git)
├── src/                   # Factor modules
│   ├── data_loader.py
│   ├── value.py
│   ├── momentum.py
│   ├── quality.py
│   ├── liquidity.py
│   ├── yield_factor.py
│   └── lowvol.py
└── outputs/               # Generated (ignored by git)
    ├── factors/
    ├── portfolio_us/
    ├── portfolio_eu/
    ├── portfolio_combined/
    └── plots/
```

- **data/** (or **DATA/**): input files; not committed (in `.gitignore`).
- **outputs/**: factor outputs and plots; not committed.
- **temp_*.xlsx**, **REDUNDANT_OR_STANDALONE.md**: also in `.gitignore`.

---

## Set up the repo and push to GitHub

### 1. Create the repository on GitHub

- Go to [github.com](https://github.com) → **New repository**.
- Name it (e.g. `minerva-code`).
- Do **not** add a README, .gitignore, or license (you have them locally).
- Click **Create repository**.

### 2. Initialise git and push (first time)

In a terminal, from the project root (the folder that contains `run_all.py` and `src/`):

```bash
cd "/Users/polina/Downloads/Minerva Code"

git init
git add .
git status
git commit -m "Initial commit: factor pipeline and notebooks"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/YOUR_REPO_NAME.git
git push -u origin main
```

- Replace **YOUR_USERNAME** with your GitHub username.
- Replace **YOUR_REPO_NAME** with the repo name you chose (e.g. `minerva-code`).

### 3. If GitHub asks for a password

Git no longer accepts account passwords for push. Use a **Personal access token**:

- GitHub → **Settings** → **Developer settings** → **Personal access tokens** → **Tokens (classic)** → **Generate new token**.
- Give it a name, tick **repo**, generate, then **copy the token**.
- When `git push` asks for a password, paste the **token** (not your GitHub password).

### 4. Later: push more changes

```bash
cd "/Users/polina/Downloads/Minerva Code"
git add .
git status
git commit -m "Short description of what you changed"
git push
```
