"""
===========================================================
Traitement : Flux de vols entre aéroports (Origin → Dest)
-----------------------------------------------------------
Calcule, pour chaque batch :
- Le nombre de vols entre chaque paire d’aéroports
Les résultats sont destinés à la collection `routes`
de la base MongoDB `flights`.

Structure :
{
  "origin": "ATL",
  "dest": "LAX",
  "flights": 128
}

Chaque batch incrémente les compteurs existants.
===========================================================
"""

import dask.dataframe as dd
from core.logger import get_logger

logger = get_logger("MetricsRoutes")


def compute_metrics_routes(ddf: dd.DataFrame) -> list:
    """
    Calcule le nombre de vols entre chaque paire d’aéroports.

    Args:
        ddf (dd.DataFrame): Batch de vols au format Dask DataFrame
                            avec les colonnes 'Origin' et 'Dest'.

    Returns:
        list[dict]: Liste de documents prêts pour MongoDB :
            [{"origin": "ATL", "dest": "LAX", "flights": 120}, ...]
    """
    try:
        # ==============================
        #  Vérification des colonnes
        # ==============================
        if not {"Origin", "Dest"}.issubset(ddf.columns):
            logger.error("❌ Colonnes 'Origin' et 'Dest' absentes du batch.")
            return []

        # ==============================
        #  Comptage des flux entre aéroports
        # ==============================
        routes_ddf = (
            ddf.groupby(["Origin", "Dest"])
            .size()
            .reset_index()
        )
        routes_ddf.columns = ["Origin", "Dest", "flights"]

        # ==============================
        #  Conversion en dictionnaire Python
        # ==============================
        routes_list = routes_ddf.compute().to_dict(orient="records")

        logger.info(f"✈️ {len(routes_list)} routes calculées dans ce batch.")
        return [
            {"origin": r["Origin"], "dest": r["Dest"], "flights": int(r["flights"])}
            for r in routes_list
        ]

    except Exception as e:
        logger.error(f"❌ Erreur lors du calcul des flux aéroportuaires : {e}")
        return []
