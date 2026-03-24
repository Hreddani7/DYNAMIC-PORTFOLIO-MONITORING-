"""
Layer 2 — Structural Correlation & Network (InteliRisk v4)
- Rolling PC1 concentration (252d) with 0.60 fragility threshold
- Full PCA decomposition with loadings + eigenvalue scree
- Correlation heatmap + 252d rolling average
- Contagion network with eigenvector + betweenness centrality
- Works on individual stock prices (not just market index)
"""
import numpy as np
import pandas as pd
from scipy.linalg import eigvals as sp_eigvals
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
import networkx as nx

from app.config import N_ANN, MARKETS

WCORR = 252

# Sector color map for network nodes
SECTOR_COLORS = {
    "Naspers": "#9878e8", "Prosus": "#9878e8", "Richemont": "#9878e8",
    "BAT": "#C9A227", "Glencore": "#E67E22",
    "AngloAmerican": "#E67E22", "BHP": "#E67E22",
    "StandardBank": "#1F4E79", "FirstRand": "#1F4E79", "Absa": "#1F4E79",
    "Nedbank": "#1F4E79", "Capitec": "#1F4E79", "Investec": "#1F4E79", "InvestecLtd": "#1F4E79",
    "Sanlam": "#2E75B6", "Discovery": "#2E75B6", "OldMutual": "#2E75B6",
    "Remgro": "#2E75B6", "Reinet": "#2E75B6",
    "AngloAmericanPlat": "#C0392B", "ImpalaPlatinum": "#C0392B",
    "GoldFields": "#C0392B", "Sibanye": "#C0392B", "AngloGold": "#C0392B",
    "KumbaIron": "#C0392B", "Exxaro": "#C0392B", "South32": "#C0392B", "Harmony": "#C0392B",
    "MTN_SA": "#20c8e0", "Vodacom": "#20c8e0",
    "Sasol": "#f09838",
    "Shoprite": "#2dd4a0", "MrPrice": "#2dd4a0", "Woolworths": "#2dd4a0",
    "TigerBrands": "#2dd4a0", "PicknPay": "#2dd4a0",
    "Aspen": "#5896f0", "Mondi": "#8d9db8", "BidCorp": "#8d9db8",
    "Growthpoint": "#d4973a", "MultiChoice": "#586880",
    "JSEBanks": "#1F4E79", "JSEMining": "#C0392B",
}


def _build_stock_prices():
    """Build a multi-column price DataFrame from individual stock data."""
    try:
        from app.ingestion import generate_stocks_per_market
        stocks = generate_stocks_per_market()
        all_stocks = {}
        for mkt, sdict in stocks.items():
            for sym, series in sdict.items():
                if len(series) >= 10:  # need some history
                    all_stocks[sym] = series
        if len(all_stocks) >= 3:
            df = pd.DataFrame(all_stocks).sort_index().dropna(how="all").ffill().bfill()
            # Clean data spikes (>15% daily moves = CSV artifacts)
            for col in df.columns:
                ret = df[col].pct_change()
                bad = ret.abs() > 0.15
                if bad.any():
                    df.loc[bad, col] = np.nan
                    df[col] = df[col].interpolate().ffill().bfill()
            return df
    except Exception:
        pass
    return None


def rolling_pc1(rpanel, mkts, win=252):
    """Rolling eigenvalue concentration — tracks PC1 through PC5 fractions."""
    n_pcs = min(5, len(mkts))
    if len(mkts) <= 1:
        idx = rpanel[mkts].dropna(how="all").index
        eff_win = min(win, max(len(idx) - 1, 1))
        dates = idx[eff_win:] if eff_win < len(idx) else idx[-1:]
        d = {"PC1_frac": [1.0] * len(dates), "PC12_frac": [1.0] * len(dates)}
        for k in range(2, n_pcs + 1):
            d[f"PC1_{k}_frac"] = [1.0] * len(dates)
        return pd.DataFrame(d, index=dates)

    data = rpanel[mkts].dropna(how="all").values
    idx = rpanel[mkts].dropna(how="all").index
    pc1_frac, pc12_frac, dates = [], [], []
    pc_cumulative = {k: [] for k in range(2, n_pcs + 1)}
    eff_win = min(win, max(len(data) - 1, 5))
    for t in range(eff_win, len(data)):
        block = data[t - eff_win:t]
        ok = np.isfinite(block).all(axis=1)
        if ok.sum() < max(eff_win // 2, 3):
            continue
        try:
            ev = np.sort(np.real(sp_eigvals(np.cov(block[ok].T))))[::-1]
            ev = ev[ev > 0]
        except Exception:
            continue
        if len(ev) == 0:
            continue
        total = ev.sum()
        pc1_frac.append(float(ev[0] / total))
        pc12_frac.append(float(ev[:2].sum() / total) if len(ev) > 1 else float(ev[0] / total))
        for k in range(2, n_pcs + 1):
            pc_cumulative[k].append(float(ev[:k].sum() / total) if len(ev) >= k else float(ev.sum() / total))
        dates.append(idx[t])
    d = {"PC1_frac": pc1_frac, "PC12_frac": pc12_frac}
    for k in range(2, n_pcs + 1):
        d[f"PC1_{k}_frac"] = pc_cumulative[k]
    return pd.DataFrame(d, index=dates)


def compute_layer2(prices, holdings):
    """Full Layer 2 — Structural analysis.

    When prices has multiple stock columns (portfolio stocks), uses those directly.
    Otherwise falls back to _build_stock_prices() for full market analysis.
    """
    # If prices already has multiple stock columns (from compute_all portfolio),
    # use them directly — this IS the portfolio's stock data
    if len(prices.columns) >= 3:
        analysis_prices = prices
        using_stocks = True
    else:
        # Single market index — try to get individual stock prices from ingestion
        stock_prices = _build_stock_prices()
        if stock_prices is not None and len(stock_prices.columns) >= 3:
            analysis_prices = stock_prices
            using_stocks = True
        else:
            analysis_prices = prices
            using_stocks = False

    ret_all = (np.log(analysis_prices / analysis_prices.shift(1)) * 100).dropna(how="all")
    min_pts = min(100, max(10, len(ret_all) // 2))
    assets_use = [c for c in ret_all.columns if ret_all[c].dropna().shape[0] >= min_pts]
    if not assets_use:
        assets_use = [c for c in ret_all.columns if ret_all[c].dropna().shape[0] >= 5]
    # Cap at 25 most liquid assets for performance
    if len(assets_use) > 25:
        counts = {c: ret_all[c].dropna().shape[0] for c in assets_use}
        assets_use = sorted(counts, key=counts.get, reverse=True)[:25]
    ret_clean = ret_all[assets_use].ffill().dropna(how="all")

    n_assets = len(assets_use)

    # ── Rolling PC1 concentration ────────────────────────────
    # Use smaller window for rolling PCA when many assets
    pca_win = min(WCORR, max(60, len(ret_clean) // 3))
    pca_conc = rolling_pc1(ret_clean, assets_use, win=pca_win)
    pc1_now = float(pca_conc["PC1_frac"].iloc[-1]) if len(pca_conc) > 0 else 0.5
    fragile = pc1_now > 0.60

    # ── Full PCA ─────────────────────────────────────────────
    ret_pca = ret_clean.dropna(how="any")
    if len(ret_pca) < 30:
        ret_pca = ret_clean.fillna(0)
    ret_sc = StandardScaler().fit_transform(ret_pca.values)
    n_comps = min(n_assets, 10)  # cap at 10 PCs for display
    pca_obj = PCA(n_components=n_comps)
    pca_obj.fit(ret_sc)
    expl = pca_obj.explained_variance_ratio_
    eigenvalues = pca_obj.explained_variance_
    comps = pca_obj.components_

    # Dynamic PC labels — name each PC by the RISK FACTOR it represents
    # Map asset types to risk factor themes (not sectors)
    FACTOR_KEYWORDS = {
        # Mining/Resources → Commodity Risk
        "GFI": "Commodity", "AGL": "Commodity", "SSW": "Commodity",
        "ANG": "Commodity", "IMP": "Commodity", "AMS": "Commodity",
        "GoldFields": "Commodity", "AngloAmerican": "Commodity", "Sibanye": "Commodity",
        "ImpalaPlatinum": "Commodity",
        # Energy → Commodity Risk
        "SOL": "Commodity", "Sasol": "Commodity",
        # Banks/Insurance → Domestic/Sovereign Risk
        "SBK": "Domestic", "FSR": "Domestic", "ABG": "Domestic", "NED": "Domestic",
        "CPI": "Domestic", "SLM": "Domestic", "DSY": "Domestic",
        "StandardBank": "Domestic", "FirstRand": "Domestic", "Absa": "Domestic",
        "Sanlam": "Domestic",
        # Tech/Global → Global Risk
        "NPN": "Global", "PRX": "Global", "CFR": "Global",
        "Naspers": "Global", "Prosus": "Global", "Richemont": "Global",
        # Telecom → Domestic
        "MTN": "Domestic", "MTN_SA": "Domestic", "Vodacom": "Domestic",
        # Retail/Consumer → Behavioral/Domestic
        "SHP": "Behavioral", "MRP": "Behavioral", "BTI": "Behavioral",
        "Shoprite": "Behavioral", "BAT": "Behavioral",
    }
    # Factor theme display names
    FACTOR_THEME_LABELS = {
        "Commodity": "Commodity Risk",
        "Domestic": "Domestic Risk",
        "Global": "Global Risk",
        "Behavioral": "Behavioral",
        "Sovereign": "Sovereign Risk",
        "USD": "USD Liquidity",
    }
    pc_labels = []
    pc_factor_pcts = []  # Store each PC's factor % impact
    for j in range(n_comps):
        abs_loads = np.abs(comps[j])
        top_idx = np.argsort(abs_loads)[::-1][:3]
        top_assets = [assets_use[k] for k in top_idx]
        # Determine risk factor theme from top-loading assets
        factors = [FACTOR_KEYWORDS.get(a, "") for a in top_assets]
        factors = [f for f in factors if f]
        if factors:
            from collections import Counter
            most_common = Counter(factors).most_common(1)[0][0]
            theme = FACTOR_THEME_LABELS.get(most_common, most_common)
            pc_labels.append(f"PC{j+1} ({theme})")
        else:
            pc_labels.append(f"PC{j+1}")
        # Store explained variance % for this PC
        pc_factor_pcts.append(round(float(expl[j]) * 100, 1))

    # PCA loadings matrix (assets × PCs)
    loadings = []
    for i, asset in enumerate(assets_use):
        short = asset[:8] if using_stocks else MARKETS.get(asset, {}).get("short", asset[:3])
        for j in range(n_comps):
            loadings.append({
                "market": short,
                "component": pc_labels[j],
                "value": round(float(comps[j, i]), 4),
            })

    # Top loadings per PC (for interpretation)
    top_loadings_per_pc = {}
    for j in range(min(n_comps, 5)):
        abs_loads = np.abs(comps[j])
        top_idx = np.argsort(abs_loads)[::-1][:5]
        top_loadings_per_pc[pc_labels[j]] = [
            {"asset": assets_use[k][:10], "loading": round(float(comps[j, k]), 3)}
            for k in top_idx
        ]

    # ── Eigenvalue scree data ────────────────────────────────
    scree = [{"pc": pc_labels[i], "eigenvalue": round(float(eigenvalues[i]), 4),
              "variance_pct": round(float(expl[i]) * 100, 2),
              "cumulative_pct": round(float(np.cumsum(expl)[i]) * 100, 2)}
             for i in range(n_comps)]

    # ── PC time series (rolling PC1 scores) ──────────────────
    pc_scores = pca_obj.transform(ret_sc)
    step_ts = max(1, len(pc_scores) // 300)
    pc_time_series = {
        "dates": [str(ret_pca.index[i].date()) if hasattr(ret_pca.index[i], 'date')
                  else str(ret_pca.index[i]) for i in range(0, len(ret_pca), step_ts)],
    }
    for j in range(min(n_comps, 5)):
        pc_time_series[pc_labels[j]] = [round(float(pc_scores[i, j]), 3)
                                         for i in range(0, len(pc_scores), step_ts)]

    # ── Correlation matrix (252d) ────────────────────────────
    corr_window = min(WCORR, len(ret_clean))
    corr_mat = ret_clean.iloc[-corr_window:].corr()
    n = len(assets_use)
    upper = np.triu_indices(n, k=1)
    avg_corr = float(np.mean(corr_mat.values[upper])) if n > 1 else 1.0

    corr_heatmap = []
    for i, m1 in enumerate(assets_use):
        short1 = m1[:8] if using_stocks else MARKETS.get(m1, {}).get("short", m1[:3])
        for j, m2 in enumerate(assets_use):
            short2 = m2[:8] if using_stocks else MARKETS.get(m2, {}).get("short", m2[:3])
            corr_heatmap.append({
                "x": short1, "y": short2,
                "value": round(float(corr_mat.iloc[i, j]), 3),
            })

    # Rolling correlation history
    corr_hist = []
    if n > 1:
        roll_step = max(1, len(ret_clean) // 150)
        for t in range(60, len(ret_clean), roll_step):
            c = ret_clean.iloc[t - 60:t].corr().values
            d = ret_clean.index[t]
            corr_hist.append({
                "date": str(d.date()) if hasattr(d, 'date') else str(d),
                "value": round(float(np.mean(c[upper])), 4),
            })

    # ── Contagion network ────────────────────────────────────
    G = nx.Graph()
    for m in assets_use:
        G.add_node(m)
    for i, m1 in enumerate(assets_use):
        for j, m2 in enumerate(assets_use):
            if i < j:
                w = abs(float(corr_mat.loc[m1, m2]))
                if w > 0.15:  # higher threshold for stocks (more edges)
                    G.add_edge(m1, m2, weight=w)

    try:
        eig_c = nx.eigenvector_centrality(G, weight="weight", max_iter=1000)
    except Exception:
        eig_c = {m: 1 / max(len(assets_use), 1) for m in assets_use}
    bet_c = nx.betweenness_centrality(G, weight="weight")

    nodes = []
    for m in assets_use:
        short = m[:8] if using_stocks else MARKETS.get(m, {}).get("short", m[:3])
        color = SECTOR_COLORS.get(m, MARKETS.get(m, {}).get("color", "#888"))
        nodes.append({
            "id": m, "short": short,
            "eigenvector": round(eig_c.get(m, 0), 4),
            "betweenness": round(bet_c.get(m, 0), 4),
            "color": color,
        })

    edges = []
    for u, v, d in G.edges(data=True):
        su = u[:8] if using_stocks else MARKETS.get(u, {}).get("short", u[:3])
        sv = v[:8] if using_stocks else MARKETS.get(v, {}).get("short", v[:3])
        edges.append({"source": su, "target": sv, "weight": round(d["weight"], 4)})

    # PC concentration history for charts — all PCs
    pc1_hist = []
    if len(pca_conc) > 0:
        step = max(1, len(pca_conc) // 200)
        for i in range(0, len(pca_conc), step):
            d = pca_conc.index[i]
            rec = {
                "date": str(d.date()) if hasattr(d, 'date') else str(d),
                "pc1": round(float(pca_conc["PC1_frac"].iloc[i]), 4),
                "pc12": round(float(pca_conc["PC12_frac"].iloc[i]), 4),
            }
            # Add cumulative fractions for PC1..PC3, PC1..PC4, PC1..PC5
            for k in range(3, 6):
                col = f"PC1_{k}_frac"
                if col in pca_conc.columns:
                    rec[f"pc1_{k}"] = round(float(pca_conc[col].iloc[i]), 4)
            pc1_hist.append(rec)

    return {
        "using_stocks": using_stocks,
        "n_assets": n_assets,
        "assets_used": assets_use,
        "pca": {
            "explained": [round(float(e), 4) for e in expl],
            "cumulative": [round(float(c), 4) for c in np.cumsum(expl)],
            "labels": pc_labels,
            "loadings": loadings,
            "top_loadings": top_loadings_per_pc,
            "pc1_explained": round(float(expl[0]), 4),
            "eigenvalues": [round(float(v), 4) for v in eigenvalues],
            "scree": scree,
            "pc_time_series": pc_time_series,
        },
        "eigen_concentration": {
            "current": round(pc1_now, 4),
            "fragile": fragile,
            "threshold": 0.60,
            "history": pc1_hist,
            "interpretation": "Diversification collapsed — systemic risk elevated"
                              if fragile else "Diversification intact",
        },
        "correlation": {
            "avg": round(avg_corr, 4),
            "matrix": corr_heatmap,
            "history": corr_hist[-200:],
        },
        "network": {
            "nodes": nodes,
            "edges": edges,
            "density": round(nx.density(G), 4),
            "clustering": round(nx.average_clustering(G), 4) if G.edges() else 0,
        },
        "_internal": {"ret_clean": ret_clean, "pca_obj": pca_obj, "pca_conc": pca_conc, "corr_mat": corr_mat},
    }
