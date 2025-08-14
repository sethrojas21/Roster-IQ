from typing import List
import pandas as pd
import numpy as np
from Analysis.config import Config

IMPACT_WEIGHTS = {
    'dporpag': 1.2,
    'porpag': 1.2,
    'ts_percent': 1.1,
}
FLIPPED_STATS_LST = ['tov_percent', 'adjde']
THRESHOLD = -0.05

def successful_transfer(bmark_plyr, plyr_stats: pd.Series, debug: bool = False) -> tuple[float, bool, float]:
    """
    Same aggregation as `successful_transfer`, but pulls the benchmark center and
    unweighted sigma from a benchmark player object. Uses the **successful_transfer**
    benchmark pack exposed by `InitBenchmarkPlayer`:
      - scalar = bmark_plyr.successful_transfer_scalar()
      - bmark_stats = bmark_plyr.successful_transfer_bmark_srs()
    Returns (score, is_successful, ess) with ess=0.0 here.
    """
    # Extract scaler and benchmark stats from the benchmark player object
    scalar = bmark_plyr.successful_transfer_scalar()
    bmark_stats_unscaled = bmark_plyr.successful_transfer_bmark_unscaled()

    if debug:
        print("Successful Transfer Benchmark Raw:")
        print(bmark_stats_unscaled)

        print("Player Success Stats")
        print(plyr_stats)

    # 1) Choose columns (intersection) and stable order (prefer scaler's fit order)
    bm_cols = set(bmark_stats_unscaled.index)
    pl_cols = set(plyr_stats.index)
    if hasattr(scalar, 'feature_names_in_') and getattr(scalar, 'feature_names_in_', None) is not None:
        fit_order = list(scalar.feature_names_in_)
        stats_columns = [c for c in fit_order if c in bm_cols and c in pl_cols]
        pos = {c: i for i, c in enumerate(fit_order)}
        idx = [pos[c] for c in stats_columns]
        sigmas = pd.Series(np.asarray(scalar.scale_)[idx], index=stats_columns, dtype=float)
    else:
        stats_columns = [c for c in bmark_stats_unscaled.index if c in pl_cols]
        sig_arr = np.asarray(getattr(scalar, 'scale_', np.ones(len(stats_columns))))
        if sig_arr.size == 1:
            sig_arr = np.repeat(sig_arr.item(), len(stats_columns))
        sigmas = pd.Series(sig_arr[:len(stats_columns)], index=stats_columns, dtype=float)

    if not stats_columns:
        return (0.0, False, 0.0)

    # 3) Per-stat z using scaler sigma
    diffs = (plyr_stats[stats_columns].astype(float) - bmark_stats_unscaled[stats_columns].astype(float))
    sigmas.replace({0.0: 1.0}, inplace=True)
    sigmas.fillna(1.0, inplace=True)
    z_series = diffs / sigmas

    # 4) Flip where lower is better
    for col in stats_columns:
        if col in FLIPPED_STATS_LST:
            z_series[col] = -z_series[col]

    # 5) Aggregate exactly like successful_transfer
    score_sum = 0.0
    weight_sum = 0.0
    for col in stats_columns:
        weight = IMPACT_WEIGHTS.get(col, 1.0)
        score_sum += float(z_series[col]) * weight
        weight_sum += weight

    score = score_sum / weight_sum if weight_sum else 0.0

    if debug:
        for col in stats_columns:
            dev = float(z_series[col])
            if abs(dev) >= 0.75:
                print(f"{col} deviation: {dev:.2f} SDs from mean")

    
    is_successful = score > THRESHOLD
    return (score, is_successful)

def effective_sample_size(weights):
    weights = np.array(weights)
    if weights.sum() == 0:
        return 0
    return (weights.sum()**2) / (weights**2).sum()
    


def testing():
    pass  # Placeholder for testing logic

if __name__ == '__main__':
    testing()