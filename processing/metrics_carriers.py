"""
===========================================================
Traitement : Statistiques par compagnie aérienne
-----------------------------------------------------------
Calcule, pour chaque batch :
- Nombre total de vols par compagnie
- Nombre de vols annulés par compagnie
- Nombre de vols retardés par compagnie
Les résultats sont destinés à la collection `carriers`
de la base MongoDB `flights`.

Structure :
{
  "carrier": "WN",
  "total": 1200,
  "cancelled": 40,
  "delayed": 280
}

Chaque batch incrémente les compteurs existants.
===========================================================
"""

import dask.dataframe as dd
from core.logger import get_logger

logger = get_logger("MetricsCarriers")


def compute_metrics_carriers(ddf: dd.DataFrame) -> list:
    """
    Calcule les statistiques de vols par compagnie dans le batch.

    Args:
        ddf (dd.DataFrame): Batch de vols au format Dask DataFrame
                            avec la colonne 'UniqueCarrier'.

    Returns:
        list[dict]: Liste de documents prêts pour MongoDB :
            [{"carrier": "WN", "total": X, "cancelled": Y, "delayed": Z}, ...]
    """
    try:
        # ==============================
        # Vérification des colonnes
        # ==============================
        required_cols = {"UniqueCarrier", "Cancelled", "ArrDelay"}
        if not required_cols.issubset(ddf.columns):
            logger.error(f"❌ Colonnes manquantes dans le batch : {required_cols - set(ddf.columns)}")
            return []

        # ==============================
        # Total des vols par compagnie
        # ==============================
        total_ddf = (
            ddf.groupby("UniqueCarrier")
            .size()
            .reset_index()
            .rename(columns={0: "total"})
        )

        # ==============================
        # Vols annulés par compagnie
        # ==============================
        cancelled_ddf = (
            ddf[ddf["Cancelled"] == 1]
            .groupby("UniqueCarrier")
            .size()
            .reset_index()
            .rename(columns={0: "cancelled"})
        )

        # ==============================
        # Vols retardés par compagnie
        # ==============================
        delayed_ddf = (
            ddf[ddf["ArrDelay"] > 0]
            .groupby("UniqueCarrier")
            .size()
            .reset_index()
            .rename(columns={0: "delayed"})
        )

        # ==============================
        # Fusion des trois DataFrames
        # ==============================
        merged = total_ddf.merge(cancelled_ddf, on="UniqueCarrier", how="left")
        merged = merged.merge(delayed_ddf, on="UniqueCarrier", how="left")

        merged = merged.fillna(value={"cancelled": 0, "delayed": 0})

        # Conversion finale
        results = merged.compute().to_dict(orient="records")

        # Renommage des clés pour MongoDB
        formatted_results = [
            {
                "carrier": r["UniqueCarrier"],
                "total": int(r["total"]),
                "cancelled": int(r["cancelled"]),
                "delayed": int(r["delayed"])
            }
            for r in results
        ]

        logger.info(f"🛫 Statistiques calculées pour {len(formatted_results)} compagnies.")
        return formatted_results

    except Exception as e:
        logger.error(f"❌ Erreur lors du calcul des statistiques compagnies : {e}")
        return []
