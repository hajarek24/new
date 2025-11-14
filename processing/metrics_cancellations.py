"""
===========================================================
Traitement : Causes d’annulation des vols
-----------------------------------------------------------
Calcule, pour chaque batch :
- Le nombre de vols annulés par cause (A, B, C, D)
Les résultats sont destinés à la collection `cancellations`
de la base MongoDB `flights`.

Structure :
{
  "cause": "A",   # Compagnie
  "count": <nombre de vols annulés>
}

Chaque batch incrémente les compteurs existants.
===========================================================
"""

import dask.dataframe as dd
from core.logger import get_logger

logger = get_logger("MetricsCancellations")


def compute_metrics_cancellations(ddf: dd.DataFrame) -> dict:
    """
    Calcule le nombre de vols annulés par cause dans le batch courant.

    Args:
        ddf (dd.DataFrame): Batch de vols au format Dask DataFrame.

    Returns:
        dict: Résultats sous la forme :
              {"A": n_A, "B": n_B, "C": n_C, "D": n_D}
    """
    try:
        # ==============================
        #  Filtrer uniquement les vols annulés
        # ==============================
        cancelled_df = ddf[ddf["Cancelled"] == 1]

        if cancelled_df.shape[0].compute() == 0:
            logger.info("🟢 Aucun vol annulé dans ce batch.")
            return {"A": 0, "B": 0, "C": 0, "D": 0}

        # ==============================
        #  Compter les causes d’annulation
        # ==============================
        cause_counts = (
            cancelled_df["CancellationCode"]
            .value_counts()
            .compute()
            .to_dict()
        )

        # ==============================
        #  Normaliser pour s’assurer que toutes les causes existent
        # ==============================
        results = {code: int(cause_counts.get(code, 0)) for code in ["A", "B", "C", "D"]}

        logger.info(f"📊 Causes d’annulation calculées : {results}")
        return results

    except Exception as e:
        logger.error(f"❌ Erreur lors du calcul des causes d’annulation : {e}")
        return {"A": 0, "B": 0, "C": 0, "D": 0}
