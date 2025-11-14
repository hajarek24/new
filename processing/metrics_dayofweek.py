"""
===========================================================
Traitement : Nombre de vols par jour de la semaine
-----------------------------------------------------------
Calcule, pour chaque batch :
- Le nombre de vols par jour (1 = lundi ... 7 = dimanche)
Les résultats sont destinés à la collection `day_of_week`
de la base MongoDB `flights`.

Structure :
{
  "day": 1,
  "count": <nombre de vols>
}

Chaque batch incrémente les compteurs existants.
===========================================================
"""

import dask.dataframe as dd
from core.logger import get_logger

logger = get_logger("MetricsDayOfWeek")


def compute_metrics_dayofweek(ddf: dd.DataFrame) -> dict:
    """
    Calcule le nombre de vols par jour de la semaine dans le batch.

    Args:
        ddf (dd.DataFrame): Batch de vols au format Dask DataFrame
                            avec la colonne 'DayOfWeek' (int de 1 à 7)

    Returns:
        dict: {1: n, 2: n, 3: n, 4: n, 5: n, 6: n, 7: n}
    """
    try:
        # ==============================
        #  Vérification de la colonne
        # ==============================
        if "DayOfWeek" not in ddf.columns:
            logger.error("❌ Colonne 'DayOfWeek' absente du batch.")
            return {i: 0 for i in range(1, 8)}

        # ==============================
        #  Comptage du nombre de vols par jour
        # ==============================
        counts = (
            ddf["DayOfWeek"]
            .value_counts()
            .compute()
            .to_dict()
        )

        # ==============================
        #  Normalisation sur 7 jours
        # ==============================
        results = {i: int(counts.get(i, 0)) for i in range(1, 8)}

        logger.info(f"📅 Répartition des vols par jour calculée : {results}")
        return results

    except Exception as e:
        logger.error(f"❌ Erreur lors du calcul des métriques temporelles : {e}")
        return {i: 0 for i in range(1, 8)}
