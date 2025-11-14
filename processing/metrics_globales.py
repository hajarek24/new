"""
===========================================================
Traitement : Statistiques globales des vols
-----------------------------------------------------------
Calcule sur chaque batch :
- Nombre total de vols
- Nombre de vols annulés
- Nombre de vols retardés
-----------------------------------------------------------
Les résultats sont destinés à la collection :
  db.stats_global.update_one(
    {"_id": "global"},
    {"$inc": {"total_flights": X, "cancelled": Y, "delayed": Z}},
    upsert=True
  )
===========================================================
"""

import dask.dataframe as dd
from core.logger import get_logger

logger = get_logger("MetricsGlobal")


def compute_metrics_global(ddf: dd.DataFrame) -> dict:
    """
    Calcule les compteurs du batch (total, annulés, retardés).

    Args:
        ddf (dd.DataFrame): Batch de vols au format Dask DataFrame.

    Returns:
        dict: Résultats sous la forme :
              {"total_flights": int, "cancelled": int, "delayed": int}
    """
    try:
        # ==============================
        #  Total de vols du batch
        # ==============================
        total_flights = ddf.shape[0].compute()

        # ==============================
        #  Vols annulés
        # ==============================
        cancelled = ddf[ddf["Cancelled"] == 1].shape[0].compute()

        # ==============================
        #  Vols retardés
        # ==============================
        delayed = ddf[ddf["ArrDelay"] > 0].shape[0].compute()

        results = {
            "total_flights": int(total_flights),
            "cancelled": int(cancelled),
            "delayed": int(delayed)
        }

        logger.info(f"✅ Métriques globales calculées : {results}")
        return results

    except Exception as e:
        logger.error(f"❌ Erreur lors du calcul des métriques globales : {e}")
        return {"total_flights": 0, "cancelled": 0, "delayed": 0}
