"""
===========================================================
Traitement : Vols entrants par aéroport et par mois
-----------------------------------------------------------
Calcule, pour chaque batch :
- Le nombre de vols entrants par aéroport (Dest)
  pour chaque combinaison (Year, Month)
Les résultats sont destinés à la collection `airports_monthly`
de la base MongoDB `flights`.

Structure :
{
  "year": 2007,
  "month": 1,
  "airport": "ATL",
  "arrivals": 120
}

Chaque batch incrémente les compteurs existants.
===========================================================
"""

import dask.dataframe as dd
from core.logger import get_logger

logger = get_logger("MetricsAirports")


def compute_metrics_airports(ddf: dd.DataFrame) -> list:
    """
    Calcule le nombre de vols entrants par aéroport (Dest)
    pour chaque couple (Year, Month).

    Args:
        ddf (dd.DataFrame): Batch de vols au format Dask DataFrame
                            avec les colonnes 'Year', 'Month', 'Dest'.

    Returns:
        list[dict]: Liste de documents prêts pour MongoDB :
            [{"year": 2007, "month": 1, "airport": "ATL", "arrivals": 120}, ...]
    """
    try:
        # ==============================
        #  Vérification des colonnes
        # ==============================
        required_cols = {"Year", "Month", "Dest"}
        if not required_cols.issubset(ddf.columns):
            logger.error(f"❌ Colonnes manquantes dans le batch : {required_cols - set(ddf.columns)}")
            return []

        # ==============================
        #  Comptage des arrivées
        # ==============================
        arrivals_ddf = (
            ddf.groupby(["Year", "Month", "Dest"])
            .size()
            .reset_index()
        )
        arrivals_ddf.columns = ["Year", "Month", "Dest", "arrivals"]

        arrivals = arrivals_ddf.compute().to_dict(orient="records")

        # ==============================
        #  Reformater pour MongoDB
        # ==============================
        formatted = [
            {
                "year": int(r["Year"]),
                "month": int(r["Month"]),
                "airport": r["Dest"],
                "arrivals": int(r["arrivals"])
            }
            for r in arrivals
        ]

        logger.info(f"Calculs terminés : {len(formatted)} enregistrements d'arrivées.")
        return formatted

    except Exception as e:
        logger.error(f"Erreur lors du calcul des métriques aéroportuaires : {e}")
        return []
