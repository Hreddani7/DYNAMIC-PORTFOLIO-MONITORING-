"""
Layer 4 — AI Regime Detection (InteliRisk v4.2)
5-state Gaussian HMM with comprehensive feature engineering.

ALL macro data from CSV injected directly into the HMM:
  VIX, DXY, MOVE, CDS, yields (SA/US), yield spreads,
  commodities (oil/gold/copper/platinum), USDZAR, repo rate, etc.

Plus portfolio risk features and factor z-scores.

States: Stable Growth, Commodity Expansion, USD Tightening,
        Sovereign Stress, Systemic Crisis.

Early regime shift detection with shift_signal and regime_duration.
Feature contribution analysis for each regime.
"""
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler

from app.config import REGIME_COUNT, REGIME_LABELS, REGIME_COLORS, FACTOR_COLS, WIN_SHORT, WIN_MED, WIN_LONG, MARKETS

try:
    from hmmlearn.hmm import GaussianHMM as _HMM
    _HAS_HMMLEARN = True
except ImportError:
    _HAS_HMMLEARN = False
    import logging as _log
    _log.getLogger(__name__).warning(
        "[L4] hmmlearn not installed — using pure-NumPy fallback. "
        "Install hmmlearn for production: pip install hmmlearn"
    )


class _FallbackHMM:
    """
    Pure-NumPy Gaussian HMM with full covariance — Baum-Welch EM + Viterbi.
    Used when hmmlearn is not available (e.g., Python 3.14 without C compiler).
    """

    def __init__(self, n_components=5, n_iter=200, tol=1e-4, random_state=0,
                 covariance_type="full", covariance_prior=None, **kw):
        self.K = n_components
        self.n_iter = n_iter
        self.tol = tol
        self.seed = random_state
        self.cov_type = covariance_type
        self.cov_prior = covariance_prior

    def _log_gauss(self, X):
        T, D = X.shape
        B = np.zeros((T, self.K))
        for k in range(self.K):
            diff = X - self.means_[k]
            if self.cov_type == "full":
                cov = self.covars_[k] + 1e-6 * np.eye(D)
                sign, logdet = np.linalg.slogdet(cov)
                if sign <= 0:
                    logdet = D * np.log(1e-6)
                inv_cov = np.linalg.inv(cov)
                maha = np.sum(diff @ inv_cov * diff, axis=1)
            else:
                var = np.maximum(self.covars_[k], 1e-6)
                logdet = np.sum(np.log(var))
                maha = np.sum(diff ** 2 / var, axis=1)
            B[:, k] = -0.5 * (D * np.log(2 * np.pi) + logdet + maha)
        return B

    def _forward(self, logB):
        T = logB.shape[0]
        logA = np.log(np.maximum(self.transmat_, 1e-300))
        logpi = np.log(np.maximum(self.startprob_, 1e-300))
        alpha = np.full((T, self.K), -np.inf)
        alpha[0] = logpi + logB[0]
        for t in range(1, T):
            alpha[t] = np.logaddexp.reduce(alpha[t - 1, :, None] + logA, axis=0) + logB[t]
        return alpha

    def _backward(self, logB):
        T = logB.shape[0]
        logA = np.log(np.maximum(self.transmat_, 1e-300))
        beta = np.zeros((T, self.K))
        for t in range(T - 2, -1, -1):
            beta[t] = np.logaddexp.reduce(logA + logB[t + 1] + beta[t + 1], axis=1)
        return beta

    def fit(self, X):
        np.random.seed(self.seed)
        T, D = X.shape
        idx = np.random.choice(T, self.K, replace=False)
        self.means_ = X[idx].copy()
        if self.cov_type == "full":
            self.covars_ = np.array([np.cov(X.T) + 1e-3 * np.eye(D)] * self.K)
            if self.cov_prior is not None:
                self.covars_ += self.cov_prior
        else:
            self.covars_ = np.array([np.var(X, axis=0)] * self.K)
        self.transmat_ = np.ones((self.K, self.K)) / self.K
        self.startprob_ = np.ones(self.K) / self.K

        prev_ll = -np.inf
        for iteration in range(self.n_iter):
            logB = self._log_gauss(X)
            alpha = self._forward(logB)
            beta = self._backward(logB)

            log_gamma = alpha + beta
            log_gamma -= np.logaddexp.reduce(log_gamma, axis=1, keepdims=True)
            gamma = np.exp(log_gamma)

            ll = float(np.logaddexp.reduce(alpha[-1]))
            if abs(ll - prev_ll) < self.tol:
                break
            prev_ll = ll

            self.startprob_ = gamma[0] / (gamma[0].sum() + 1e-10)
            gs = gamma.sum(axis=0) + 1e-10
            self.means_ = (gamma.T @ X) / gs[:, None]

            logA = np.log(np.maximum(self.transmat_, 1e-300))
            for t in range(T - 1):
                log_xi = alpha[t, :, None] + logA + logB[t + 1, None, :] + beta[t + 1, None, :] - ll
                if t == 0:
                    xi_acc = np.exp(log_xi)
                else:
                    xi_acc += np.exp(log_xi)
            self.transmat_ = xi_acc
            self.transmat_ /= self.transmat_.sum(axis=1, keepdims=True) + 1e-10

            for k in range(self.K):
                diff = X - self.means_[k]
                if self.cov_type == "full":
                    w = gamma[:, k:k + 1]
                    self.covars_[k] = (diff * w).T @ diff / gs[k]
                    if self.cov_prior is not None:
                        self.covars_[k] += self.cov_prior * 0.01
                    self.covars_[k] += 1e-4 * np.eye(D)
                else:
                    self.covars_[k] = (gamma[:, k] @ (diff ** 2)) / gs[k] + 1e-4

        self._ll = prev_ll
        return self

    def predict(self, X):
        logB = self._log_gauss(X)
        logA = np.log(np.maximum(self.transmat_, 1e-300))
        logpi = np.log(np.maximum(self.startprob_, 1e-300))
        T = X.shape[0]
        delta = np.full((T, self.K), -np.inf)
        psi = np.zeros((T, self.K), dtype=int)
        delta[0] = logpi + logB[0]
        for t in range(1, T):
            for k in range(self.K):
                sc = delta[t - 1] + logA[:, k]
                psi[t, k] = np.argmax(sc)
                delta[t, k] = sc[psi[t, k]] + logB[t, k]
        path = np.zeros(T, dtype=int)
        path[-1] = np.argmax(delta[-1])
        for t in range(T - 2, -1, -1):
            path[t] = psi[t + 1, path[t + 1]]
        return path

    def predict_proba(self, X):
        logB = self._log_gauss(X)
        alpha = self._forward(logB)
        beta = self._backward(logB)
        lg = alpha + beta
        lg -= np.logaddexp.reduce(lg, axis=1, keepdims=True)
        return np.exp(lg)

    def score(self, X):
        logB = self._log_gauss(X)
        alpha = self._forward(logB)
        return float(np.logaddexp.reduce(alpha[-1]))


def _rzs(s, w=63, min_p=21):
    """Rolling z-score with relaxed min_periods."""
    mu = s.rolling(w, min_periods=min_p).mean()
    sd = s.rolling(w, min_periods=min_p).std()
    return (s - mu) / (sd + 1e-8)


def forecast_regime(trans, p0, steps):
    p = np.array(p0, dtype=float)
    for _ in range(steps):
        p = p @ trans
    return int(np.argmax(p)), p.tolist()


def _label_regimes(X_hmm, states, feat_names, K):
    """
    Label regimes using economic characteristics.
    Uses macro data features (VIX, CDS, yields, commodities) for proper identification.
    """
    # Find feature indices by category
    vol_idxs = [i for i, n in enumerate(feat_names) if "rv_" in n or "vol" in n.lower()]
    ret_idxs = [i for i, n in enumerate(feat_names) if "ret_" in n]
    commodity_idxs = [i for i, n in enumerate(feat_names)
                      if any(x in n.lower() for x in ["F3", "commod", "oil", "gold", "copper", "platinum"])]
    usd_idxs = [i for i, n in enumerate(feat_names)
                if any(x in n.lower() for x in ["F2", "dxy", "usdzar"])]
    sovereign_idxs = [i for i, n in enumerate(feat_names)
                      if any(x in n.lower() for x in ["F4", "sov", "cds", "yield_spread", "sa_us_spread"])]
    vix_idxs = [i for i, n in enumerate(feat_names) if "vix" in n.lower()]
    crisis_idxs = vol_idxs + vix_idxs + [i for i, n in enumerate(feat_names) if "cds" in n.lower()]

    state_profiles = []
    for k in range(K):
        mask = states == k
        if mask.sum() == 0:
            state_profiles.append({"vol": 0, "ret": 0, "commodity": 0, "usd": 0,
                                   "sovereign": 0, "vix": 0, "crisis": 0, "count": 0})
            continue
        vol_mean = X_hmm[mask][:, vol_idxs].mean() if vol_idxs else 0
        ret_mean = X_hmm[mask][:, ret_idxs].mean() if ret_idxs else 0
        com_mean = X_hmm[mask][:, commodity_idxs].mean() if commodity_idxs else 0
        usd_mean = X_hmm[mask][:, usd_idxs].mean() if usd_idxs else 0
        sov_mean = X_hmm[mask][:, sovereign_idxs].mean() if sovereign_idxs else 0
        vix_mean = X_hmm[mask][:, vix_idxs].mean() if vix_idxs else 0
        crisis_mean = X_hmm[mask][:, crisis_idxs].mean() if crisis_idxs else 0
        state_profiles.append({
            "vol": vol_mean, "ret": ret_mean, "commodity": com_mean,
            "usd": usd_mean, "sovereign": sov_mean, "vix": vix_mean,
            "crisis": crisis_mean, "count": int(mask.sum()),
        })

    # Score each state for each regime archetype
    # Stable Growth: low vol, low VIX, positive returns, low CDS
    # Commodity Expansion: positive commodity, moderate vol
    # USD Tightening: high USD/DXY, moderate-high vol
    # Sovereign Stress: high sovereign/CDS/yield spread, high vol
    # Systemic Crisis: very high vol, very high VIX, very high CDS, negative returns
    scores = np.zeros((K, K))
    for k in range(K):
        p = state_profiles[k]
        scores[k, 0] = -p["vol"] - p["vix"] + p["ret"] - p["sovereign"]  # Stable Growth
        scores[k, 1] = p["commodity"] - abs(p["vol"]) * 0.3              # Commodity Expansion
        scores[k, 2] = p["usd"] + p["vol"] * 0.3                         # USD Tightening
        scores[k, 3] = p["sovereign"] + p["vol"] * 0.3 + p["vix"] * 0.2  # Sovereign Stress
        scores[k, 4] = p["crisis"] * 1.5 + p["vol"] * 1.5 - p["ret"]    # Systemic Crisis

    # Hungarian-style assignment: greedy matching
    used_states = set()
    used_regimes = set()
    order = [0] * K

    for _ in range(K):
        best_val = -np.inf
        best_s, best_r = 0, 0
        for s in range(K):
            if s in used_states:
                continue
            for r in range(K):
                if r in used_regimes:
                    continue
                if scores[s, r] > best_val:
                    best_val = scores[s, r]
                    best_s, best_r = s, r
        order[best_r] = best_s
        used_states.add(best_s)
        used_regimes.add(best_r)

    return order


# ── Feature category names for contribution analysis ──────────
FEAT_CATEGORIES = {
    "market_risk": ["rv_", "vol", "disp_"],
    "structural": ["pc1_conc", "corr_mean"],
    "usd_liquidity": ["F2_USD", "dxy", "usdzar"],
    "commodity": ["F3_Commodity", "oil", "gold", "copper", "platinum"],
    "sovereign": ["F4_Sovereign", "cds", "yield_spread", "sa_us_spread", "yield_sa"],
    "behavioral": ["F6_Herding"],
    "global": ["vix", "msci_em", "move", "F1_Global", "eurostoxx"],
}


def _categorize_feature(feat_name):
    """Map a feature name to its category."""
    fn_lower = feat_name.lower()
    for cat, patterns in FEAT_CATEGORIES.items():
        for p in patterns:
            if p.lower() in fn_lower:
                return cat
    if "drawdown" in fn_lower:
        return "market_risk"
    if "ret_" in fn_lower:
        return "market_risk"
    if "fed_funds" in fn_lower or "repo" in fn_lower or "jibar" in fn_lower:
        return "sovereign"
    return "market_risk"


def _safe_macro_feature(macro_series, common_idx, name, min_pts=50):
    """Align a macro series to common_idx, compute rolling z-score, return (series, name) or None."""
    if macro_series is None or len(macro_series) < 20:
        return None
    # Align
    s = macro_series.reindex(common_idx).ffill().bfill()
    if s.notna().sum() < min_pts:
        return None
    # Rolling z-score (63d)
    mu = s.rolling(63, min_periods=21).mean()
    sd = s.rolling(63, min_periods=21).std()
    zs = (s - mu) / (sd + 1e-8)
    zs = zs.fillna(0)
    return zs


def _safe_macro_ret(macro_series, common_idx, name, min_pts=50):
    """Convert a macro price series to log returns, align, z-score."""
    if macro_series is None or len(macro_series) < 20:
        return None
    s = macro_series.reindex(common_idx).ffill().bfill()
    r = np.log(s.replace(0, np.nan) / s.replace(0, np.nan).shift(1)) * 100
    r = r.fillna(0)
    mu = r.rolling(63, min_periods=21).mean()
    sd = r.rolling(63, min_periods=21).std()
    zs = (r - mu) / (sd + 1e-8)
    zs = zs.fillna(0)
    if zs.notna().sum() < min_pts:
        return None
    return zs


def compute_layer4(prices, holdings, l0_internal=None, l2_internal=None, l3_internal=None):
    """Full Layer 4 — HMM regime detection with ALL macro data + portfolio features."""
    from app.ingestion import generate_macro

    macro_df = generate_macro()

    ret_all = (np.log(prices / prices.shift(1)) * 100).dropna(how="all")
    min_pts = min(100, max(10, len(ret_all) // 2))
    markets_use = [c for c in ret_all.columns if ret_all[c].dropna().shape[0] >= min_pts]
    if not markets_use:
        markets_use = [c for c in ret_all.columns if ret_all[c].dropna().shape[0] >= 5]
    ret_clean = ret_all[markets_use].ffill().dropna(how="all")
    common_idx = ret_clean.index

    hmm_feat = pd.DataFrame(index=common_idx)
    feat_names = []

    # Helper to add a feature
    def _add(name, series):
        if series is not None and len(series) > 0:
            hmm_feat[name] = series.reindex(common_idx).fillna(0)
            feat_names.append(name)

    # ═══════════════════════════════════════════════════════════
    # CATEGORY 1: Market Risk Features (portfolio vol, dispersion, drawdown)
    # ═══════════════════════════════════════════════════════════
    use_portfolio_agg = len(markets_use) > 4
    if use_portfolio_agg:
        port_ret = ret_clean[markets_use].mean(axis=1)
        cross_vol = ret_clean[markets_use].std(axis=1)
        for wn, w in [("21d", WIN_SHORT), ("63d", WIN_MED)]:
            mp = max(min(w // 2, len(port_ret) // 2), 2)
            v = port_ret.rolling(w, min_periods=mp).std() * np.sqrt(252)
            _add(f"rv_pf_{wn}", (v - v.mean()) / (v.std() + 1e-8))
            cv = cross_vol.rolling(w, min_periods=mp).mean()
            _add(f"disp_{wn}", (cv - cv.mean()) / (cv.std() + 1e-8))
    else:
        for mkt in markets_use:
            r = ret_clean[mkt]
            for wn, w in [("21d", WIN_SHORT), ("63d", WIN_MED)]:
                mp = max(min(w // 2, len(r) // 2), 2)
                v = r.rolling(w, min_periods=mp).std() * np.sqrt(252)
                _add(f"rv_{mkt[:3]}_{wn}", (v - v.mean()) / (v.std() + 1e-8))

    # Return direction (portfolio-level)
    if use_portfolio_agg:
        port_ret_dir = ret_clean[markets_use].mean(axis=1)
        rm = port_ret_dir.rolling(21, min_periods=10).mean()
        _add("ret_pf_21d", (rm - rm.mean()) / (rm.std() + 1e-8))

    # Drawdown
    port_ret = ret_clean[markets_use].mean(axis=1)
    cum = (port_ret / 100).cumsum()
    dd = cum - cum.cummax()
    _add("drawdown", (dd - dd.mean()) / (dd.std() + 1e-8))

    # ═══════════════════════════════════════════════════════════
    # CATEGORY 2: Structural Features (PC1, correlation)
    # ═══════════════════════════════════════════════════════════
    if l2_internal and "pca_conc" in l2_internal and len(l2_internal["pca_conc"]) > 0:
        pc1 = l2_internal["pca_conc"]["PC1_frac"].reindex(common_idx).ffill().bfill()
        if pc1.notna().sum() > 50:
            _add("pc1_conc", (pc1 - pc1.mean()) / (pc1.std() + 1e-8))

    if len(markets_use) > 1:
        corr_parts = []
        for i, m1 in enumerate(markets_use):
            for j, m2 in enumerate(markets_use):
                if i < j:
                    s1 = ret_clean[m1].dropna()
                    s2 = ret_clean[m2].dropna()
                    common = s1.index.intersection(s2.index)
                    if len(common) >= 30:
                        rc = s1.loc[common].rolling(min(60, len(common) // 2), min_periods=20).corr(s2.loc[common])
                        corr_parts.append(rc)
        if corr_parts:
            corr_mean = pd.concat(corr_parts, axis=1).reindex(common_idx).ffill().bfill().mean(axis=1).fillna(0)
            _add("corr_mean", (corr_mean - corr_mean.mean()) / (corr_mean.std() + 1e-8))

    # ═══════════════════════════════════════════════════════════
    # CATEGORY 3: DIRECT MACRO DATA — ALL CSV series injected into HMM
    # This is the KEY change: raw macro data drives regime detection
    # ═══════════════════════════════════════════════════════════
    if macro_df is not None and len(macro_df) > 0:
        macro = macro_df

        # --- Global Risk Indicators ---
        # VIX (level z-score — high VIX = risk-off)
        if "vix" in macro.columns:
            zs = _safe_macro_feature(macro["vix"], common_idx, "vix")
            if zs is not None:
                _add("vix_level", zs)

        # MSCI EM (returns — falling EM = risk)
        if "msci_em" in macro.columns:
            zs = _safe_macro_ret(macro["msci_em"], common_idx, "msci_em")
            if zs is not None:
                _add("msci_em_ret", zs)

        # MOVE Index (bond volatility — high = stress)
        if "move" in macro.columns:
            zs = _safe_macro_feature(macro["move"], common_idx, "move")
            if zs is not None:
                _add("move_level", zs)

        # Euro Stoxx (returns)
        if "eurostoxx" in macro.columns:
            zs = _safe_macro_ret(macro["eurostoxx"], common_idx, "eurostoxx")
            if zs is not None:
                _add("eurostoxx_ret", zs)

        # --- USD Liquidity ---
        # DXY (level z-score — high DXY = USD tightening)
        if "dxy" in macro.columns:
            zs = _safe_macro_feature(macro["dxy"], common_idx, "dxy")
            if zs is not None:
                _add("dxy_level", zs)

        # USDZAR (level — high = ZAR weakness)
        if "USDZAR" in macro.columns:
            zs = _safe_macro_feature(macro["USDZAR"], common_idx, "USDZAR")
            if zs is not None:
                _add("usdzar_level", zs)

        # Fed Funds Rate
        if "fed_funds" in macro.columns:
            zs = _safe_macro_feature(macro["fed_funds"], common_idx, "fed_funds")
            if zs is not None:
                _add("fed_funds_level", zs)

        # --- Commodity Risk ---
        # Oil (returns)
        if "oil" in macro.columns:
            zs = _safe_macro_ret(macro["oil"], common_idx, "oil")
            if zs is not None:
                _add("oil_ret", zs)

        # Gold (returns)
        if "gold" in macro.columns:
            zs = _safe_macro_ret(macro["gold"], common_idx, "gold")
            if zs is not None:
                _add("gold_ret", zs)

        # Copper (returns)
        if "copper" in macro.columns:
            zs = _safe_macro_ret(macro["copper"], common_idx, "copper")
            if zs is not None:
                _add("copper_ret", zs)

        # Platinum (returns)
        if "platinum" in macro.columns:
            zs = _safe_macro_ret(macro["platinum"], common_idx, "platinum")
            if zs is not None:
                _add("platinum_ret", zs)

        # --- Sovereign Risk (CRITICAL — CDS, yields, spreads) ---
        # SA CDS 5Y (level — high = sovereign distress)
        if "cds_SA" in macro.columns:
            zs = _safe_macro_feature(macro["cds_SA"], common_idx, "cds_SA")
            if zs is not None:
                _add("cds_sa_level", zs)

        # SA 10Y Yield (level)
        if "yield_SA" in macro.columns:
            zs = _safe_macro_feature(macro["yield_SA"], common_idx, "yield_SA")
            if zs is not None:
                _add("yield_sa_level", zs)

        # US 10Y Yield (level)
        if "yield_US" in macro.columns:
            zs = _safe_macro_feature(macro["yield_US"], common_idx, "yield_US")
            if zs is not None:
                _add("yield_us_level", zs)

        # SA-US Yield Spread (SOVEREIGN FACTOR — calculated here)
        if "yield_SA" in macro.columns and "yield_US" in macro.columns:
            sa_yield = macro["yield_SA"].reindex(common_idx).ffill().bfill()
            us_yield = macro["yield_US"].reindex(common_idx).ffill().bfill()
            sa_us_spread = sa_yield - us_yield
            zs = _safe_macro_feature(sa_us_spread, common_idx, "sa_us_spread")
            if zs is not None:
                _add("sa_us_yield_spread", zs)

        # SA Yield Curve Spread (10Y-2Y)
        if "yield_spread_SA" in macro.columns:
            zs = _safe_macro_feature(macro["yield_spread_SA"], common_idx, "yield_spread_SA")
            if zs is not None:
                _add("yield_spread_sa", zs)

        # SA 2Y Yield
        if "yield_SA_2Y" in macro.columns:
            zs = _safe_macro_feature(macro["yield_SA_2Y"], common_idx, "yield_SA_2Y")
            if zs is not None:
                _add("yield_sa_2y", zs)

        # SA 20Y Yield
        if "yield_SA_20Y" in macro.columns:
            zs = _safe_macro_feature(macro["yield_SA_20Y"], common_idx, "yield_SA_20Y")
            if zs is not None:
                _add("yield_sa_20y", zs)

        # --- Domestic Macro ---
        # Repo Rate
        if "repo_rate" in macro.columns:
            zs = _safe_macro_feature(macro["repo_rate"], common_idx, "repo_rate")
            if zs is not None:
                _add("repo_rate_level", zs)

        # JIBAR
        if "jibar" in macro.columns:
            zs = _safe_macro_feature(macro["jibar"], common_idx, "jibar")
            if zs is not None:
                _add("jibar_level", zs)

    # ═══════════════════════════════════════════════════════════
    # CATEGORY 4: Factor z-scores from L3 (supplement macro data)
    # ═══════════════════════════════════════════════════════════
    factor_features = ["F1_Global", "F2_USD", "F3_Commodity", "F4_Sovereign", "F5_Domestic", "F6_Herding"]
    if l3_internal and "fact_sc" in l3_internal:
        for f in factor_features:
            if f in l3_internal["fact_sc"].columns:
                s = l3_internal["fact_sc"][f].reindex(common_idx).ffill().bfill()
                if s.notna().sum() > 50:
                    _add(f, s)

    # ═══════════════════════════════════════════════════════════
    # Build feature matrix
    # ═══════════════════════════════════════════════════════════
    hmm_feat = hmm_feat.ffill().bfill()
    hmm_feat = hmm_feat.dropna()
    if len(hmm_feat) < 50:
        return _default_regime(markets_use)

    # Limit feature count to avoid curse of dimensionality
    # If too many features, select the most informative ones
    MAX_FEATURES = 25
    if len(feat_names) > MAX_FEATURES:
        # Keep all market risk + structural features, then sample from macro
        priority = []
        secondary = []
        for i, n in enumerate(feat_names):
            cat = _categorize_feature(n)
            if cat in ("market_risk", "structural"):
                priority.append(i)
            elif n in ("vix_level", "cds_sa_level", "sa_us_yield_spread", "dxy_level",
                        "oil_ret", "gold_ret", "usdzar_level", "yield_sa_level",
                        "F1_Global", "F2_USD", "F3_Commodity", "F4_Sovereign", "F6_Herding"):
                priority.append(i)
            else:
                secondary.append(i)
        # Fill remaining slots from secondary
        remaining = MAX_FEATURES - len(priority)
        if remaining > 0:
            selected = priority + secondary[:remaining]
        else:
            selected = priority[:MAX_FEATURES]
        selected = sorted(selected)
        feat_names = [feat_names[i] for i in selected]
        hmm_feat = hmm_feat.iloc[:, selected]

    X_hmm = StandardScaler().fit_transform(hmm_feat.values)
    D = X_hmm.shape[1]
    K = REGIME_COUNT

    print(f"[L4] HMM features ({D}): {feat_names}")

    # ═══════════════════════════════════════════════════════════
    # Fit HMM
    # ═══════════════════════════════════════════════════════════
    HMM_CLS = _HMM if _HAS_HMMLEARN else _FallbackHMM
    best_ll = -np.inf
    best_model = None

    n_seeds = 20 if _HAS_HMMLEARN else 5
    n_iter_full = 200 if _HAS_HMMLEARN else 80
    n_iter_diag = 300 if _HAS_HMMLEARN else 100

    for seed in range(n_seeds):
        try:
            kw = dict(
                n_components=K,
                covariance_type="full",
                n_iter=n_iter_full,
                tol=1e-4,
                random_state=seed * 7 + 1,
                covariance_prior=np.eye(D) * 0.1,
            )
            if _HAS_HMMLEARN:
                kw["init_params"] = "stmc"
                kw["params"] = "stmc"
            m = HMM_CLS(**kw)
            m.fit(X_hmm)
            seq = m.predict(X_hmm)
            if max((seq == k).mean() for k in range(K)) > 0.90:
                continue
            ll = m.score(X_hmm)
            if ll > best_ll:
                best_ll = ll
                best_model = m
        except Exception:
            continue

    # Fallback: diag covariance
    if best_model is None:
        for seed in range(n_seeds):
            try:
                m = HMM_CLS(
                    n_components=K,
                    covariance_type="diag",
                    n_iter=n_iter_diag,
                    tol=1e-5,
                    random_state=seed * 7 + 1,
                )
                m.fit(X_hmm)
                seq = m.predict(X_hmm)
                if max((seq == k).mean() for k in range(K)) > 0.90:
                    continue
                ll = m.score(X_hmm)
                if ll > best_ll:
                    best_ll = ll
                    best_model = m
            except Exception:
                continue

    if best_model is None:
        return _default_regime(markets_use)

    # ═══════════════════════════════════════════════════════════
    # Decode and label states
    # ═══════════════════════════════════════════════════════════
    raw_labels = best_model.predict(X_hmm)
    raw_probs = best_model.predict_proba(X_hmm)

    order = _label_regimes(X_hmm, raw_labels, feat_names, K)
    remap = {old: new for new, old in enumerate(order)}

    states = np.array([remap[s] for s in raw_labels])
    probs = raw_probs[:, order]
    trans_m = best_model.transmat_[order][:, order]

    regime_s = pd.Series(states, index=hmm_feat.index, name="regime")
    reg_probs = pd.DataFrame(
        probs,
        index=hmm_feat.index,
        columns=[REGIME_LABELS[i] for i in range(K)],
    )

    cur_reg = int(regime_s.iloc[-1])
    cur_probs = reg_probs.iloc[-1]
    crisis_p = float(cur_probs.get("Sovereign Stress", 0) + cur_probs.get("Systemic Crisis", 0))

    print(f"[L4] Active regime: {REGIME_LABELS[cur_reg]} | Crisis P: {crisis_p:.3f}")

    # ═══════════════════════════════════════════════════════════
    # Regime Duration & Shift Detection
    # ═══════════════════════════════════════════════════════════
    regime_duration = 0
    for i in range(len(regime_s) - 1, -1, -1):
        if regime_s.iloc[i] == cur_reg:
            regime_duration += 1
        else:
            break

    shift_prob = 1.0 - float(trans_m[cur_reg, cur_reg])

    if len(reg_probs) > 21:
        recent_stay = float(reg_probs.iloc[-5:][REGIME_LABELS[cur_reg]].mean())
        older_stay = float(reg_probs.iloc[-21:-5][REGIME_LABELS[cur_reg]].mean())
        shift_accel = older_stay - recent_stay
    else:
        shift_accel = 0

    if shift_prob > 0.4 or (shift_prob > 0.25 and shift_accel > 0.1):
        shift_signal = "imminent"
    elif shift_prob > 0.25 or shift_accel > 0.1:
        shift_signal = "warning"
    elif shift_prob > 0.15 or shift_accel > 0.05:
        shift_signal = "watch"
    else:
        shift_signal = "stable"

    # ═══════════════════════════════════════════════════════════
    # Feature Contribution Analysis
    # ═══════════════════════════════════════════════════════════
    feature_contributions = {}
    last_row = X_hmm[-1]
    all_cats = set(FEAT_CATEGORIES.keys())
    all_cats.add("global")
    for cat in all_cats:
        cat_idxs = [i for i, n in enumerate(feat_names) if _categorize_feature(n) == cat]
        if cat_idxs:
            avg_abs = float(np.mean(np.abs(last_row[cat_idxs])))
            if avg_abs > 1.5:
                impact = "high"
            elif avg_abs > 0.75:
                impact = "moderate"
            else:
                impact = "low"
            feature_contributions[cat] = {"value": round(avg_abs, 3), "impact": impact}
        else:
            feature_contributions[cat] = {"value": 0.0, "impact": "low"}

    # ═══════════════════════════════════════════════════════════
    # Regime Characteristics
    # ═══════════════════════════════════════════════════════════
    regime_characteristics = {}
    for r in range(K):
        mask = regime_s == r
        count = int(mask.sum())
        if count == 0:
            regime_characteristics[REGIME_LABELS[r]] = {
                "description": REGIME_LABELS[r],
                "typical_duration": 0, "key_drivers": [],
            }
            continue
        streaks = []
        cur_streak = 0
        for v in regime_s.values:
            if v == r:
                cur_streak += 1
            else:
                if cur_streak > 0:
                    streaks.append(cur_streak)
                cur_streak = 0
        if cur_streak > 0:
            streaks.append(cur_streak)
        avg_dur = float(np.mean(streaks)) if streaks else 0

        regime_means = X_hmm[mask.values].mean(axis=0)
        top_feat_idxs = np.argsort(np.abs(regime_means))[::-1][:3]
        key_drivers = [feat_names[i] for i in top_feat_idxs]

        descriptions = {
            0: "Low volatility, positive returns. Markets trending upward with contained risk.",
            1: "Commodity prices driving markets. Mining and resource sectors outperform.",
            2: "USD strength pressuring EM currencies. Capital outflows, ZAR weakness.",
            3: "Elevated sovereign risk premiums. Political or fiscal uncertainty. High CDS spreads.",
            4: "Extreme volatility, VIX spike, CDS blowout, correlation surge. Systemic risk-off.",
        }
        regime_characteristics[REGIME_LABELS[r]] = {
            "description": descriptions.get(r, REGIME_LABELS[r]),
            "typical_duration": round(avg_dur, 1),
            "key_drivers": key_drivers,
        }

    # ═══════════════════════════════════════════════════════════
    # Forecasts
    # ═══════════════════════════════════════════════════════════
    fc1_id, fc1_p = forecast_regime(trans_m, cur_probs.values, 1)
    fc5_id, fc5_p = forecast_regime(trans_m, cur_probs.values, 5)
    fc21_id, fc21_p = forecast_regime(trans_m, cur_probs.values, 21)

    # ═══════════════════════════════════════════════════════════
    # History
    # ═══════════════════════════════════════════════════════════
    history = []
    step = max(1, len(regime_s) // 200)
    for i in range(0, len(regime_s), step):
        d = regime_s.index[i]
        rec = {"date": str(d.date()) if hasattr(d, "date") else str(d), "regime": REGIME_LABELS[regime_s.iloc[i]]}
        for r in range(K):
            rec[REGIME_LABELS[r]] = round(float(reg_probs.iloc[i, r]), 4)
        history.append(rec)

    # ═══════════════════════════════════════════════════════════
    # Regime stats
    # ═══════════════════════════════════════════════════════════
    regime_stats = {}
    vol_cols = [c for c in hmm_feat.columns if "rv_" in c and "21d" in c]
    for r in range(K):
        mask = regime_s == r
        if mask.sum() > 0:
            regime_stats[REGIME_LABELS[r]] = {
                "count": int(mask.sum()),
                "pct": round(float(mask.mean()) * 100, 1),
                "avg_vol": round(float(hmm_feat[vol_cols].values[mask.values].mean()), 4) if vol_cols else 0,
            }

    # Transition matrix dict
    tm = {}
    for i in range(K):
        tm[REGIME_LABELS[i]] = {REGIME_LABELS[j]: round(float(trans_m[i, j]), 4) for j in range(K)}

    n_params = K * D + K * D * D + K - 1
    aic = -2 * best_ll + 2 * n_params
    bic = -2 * best_ll + np.log(len(X_hmm)) * n_params

    return {
        "active_regime": REGIME_LABELS[cur_reg],
        "active_idx": cur_reg,
        "regime_probs": {REGIME_LABELS[r]: round(float(cur_probs.iloc[r]), 4) for r in range(K)},
        "crisis_probability": round(crisis_p, 4),
        "regime_duration": regime_duration,
        "shift_probability": round(shift_prob, 4),
        "shift_signal": shift_signal,
        "feature_contributions": feature_contributions,
        "regime_characteristics": regime_characteristics,
        "forecast": {
            "+1d": {"regime": REGIME_LABELS[fc1_id], "probs": {REGIME_LABELS[i]: round(fc1_p[i], 4) for i in range(K)}},
            "+5d": {"regime": REGIME_LABELS[fc5_id], "probs": {REGIME_LABELS[i]: round(fc5_p[i], 4) for i in range(K)}},
            "+21d": {"regime": REGIME_LABELS[fc21_id], "probs": {REGIME_LABELS[i]: round(fc21_p[i], 4) for i in range(K)}},
        },
        "transition_matrix": tm,
        "regime_stats": regime_stats,
        "history": history,
        "model_info": {
            "type": "GaussianHMM (hmmlearn)" if _HAS_HMMLEARN else "Fallback",
            "best_ll": round(best_ll, 2),
            "aic": round(aic, 1),
            "bic": round(bic, 1),
            "n_features": D,
            "n_observations": len(X_hmm),
            "feature_names": feat_names,
            "feature_categories": {cat: [feat_names[i] for i in range(len(feat_names))
                                          if _categorize_feature(feat_names[i]) == cat]
                                   for cat in all_cats},
        },
        "regime_labels": REGIME_LABELS,
        "regime_colors": REGIME_COLORS,
        "_internal": {"regime_s": regime_s, "reg_probs": reg_probs, "trans_m": trans_m},
    }


def _default_regime(markets_use):
    return {
        "active_regime": "Stable Growth", "active_idx": 0,
        "regime_probs": {REGIME_LABELS[r]: 0.2 for r in range(REGIME_COUNT)},
        "crisis_probability": 0.0,
        "regime_duration": 0, "shift_probability": 0.0, "shift_signal": "stable",
        "feature_contributions": {}, "regime_characteristics": {},
        "forecast": {"+1d": {"regime": "Stable Growth", "probs": {}},
                     "+5d": {"regime": "Stable Growth", "probs": {}},
                     "+21d": {"regime": "Stable Growth", "probs": {}}},
        "transition_matrix": {}, "regime_stats": {}, "history": [],
        "model_info": {"error": "Insufficient data or hmmlearn not installed"},
        "regime_labels": REGIME_LABELS, "regime_colors": REGIME_COLORS,
        "_internal": {},
    }
