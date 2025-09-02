from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
import libsql
import os
from dotenv import load_dotenv
import numpy as np
import pandas as pd
from Analysis.CalculateScores.calcCompositeScore import composite_score as cs_score

load_dotenv()
app = FastAPI()

@app.get("/")
def root():
    return {"ok": True}

def to_jsonable(x):
    # pandas
    if isinstance(x, pd.DataFrame):
        return x.to_dict(orient="records")
    if isinstance(x, pd.Series):
        return x.to_dict()

    # numpy
    if isinstance(x, (np.integer,)):
        return int(x)
    if isinstance(x, (np.floating,)):
        return float(x)
    if isinstance(x, (np.ndarray,)):
        return x.tolist()

    # libsql connection/cursor → drop/str/null
    if isinstance(x, libsql.Connection):
        return None

    # composites
    if isinstance(x, dict):
        return {k: to_jsonable(v) for k, v in x.items() if not isinstance(v, libsql.Connection)}
    if isinstance(x, (list, tuple, set)):
        return [to_jsonable(v) for v in x]

    # primitives
    return x  # str/int/float/bool/None should pass

@app.get("/compute")
async def composite_score(team_name: str, season_year: int, player_id_to_replace: int):
    url = os.getenv("TURSO_URL")
    auth_token = os.getenv("TURSO_AUTH_TOKEN")
    if not url or not auth_token:
        raise HTTPException(status_code=500, detail="Database configuration missing")

    try:
        # remote-only connection (no replica, no sync)
        conn = libsql.connect(url, auth_token=auth_token)

        bmark_plyr, cs_df = cs_score(conn, team_name, season_year, player_id_to_replace)

        payload = {
            "benchmark_player": to_jsonable(bmark_plyr),
            "composite_scores": to_jsonable(cs_df),
        }
        # Extra safety: tell FastAPI how to encode any leftovers
        encoded = jsonable_encoder(payload, custom_encoder={
            libsql.Connection: lambda _: None,
            pd.DataFrame: lambda df: df.to_dict(orient="records"),
            np.integer: int,
            np.floating: float,
            np.ndarray: lambda a: a.tolist(),
        })
        return JSONResponse(content=encoded)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        try:
            conn.close()
        except Exception:
            pass