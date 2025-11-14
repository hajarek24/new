"""
===========================================================
Dask Cluster Manager
-----------------------------------------------------------
Lance un cluster Dask local et exécute des tâches distribuées
sur les batches Kafka transformés en DataFrame.
===========================================================
"""

from dask.distributed import Client, LocalCluster
from core.logger import get_logger
from config.settings import DASK_N_WORKERS, DASK_THREADS_PER_WORKER

logger = get_logger("DaskClusterManager")


class DaskClusterManager:
    def __init__(self):
        self.client = None

    def start_cluster(self):
        """
        Lance un cluster Dask local.
        """
        try:
            logger.info("🖥️ Lancement d’un cluster Dask local...")
            cluster = LocalCluster(
                n_workers=DASK_N_WORKERS,
                threads_per_worker=DASK_THREADS_PER_WORKER,
                dashboard_address=None,  # pas besoin du dashboard ici
                # idle_timeout a été retiré dans les versions récentes
            )
            self.client = Client(cluster)
            logger.info(f"✅ Cluster Dask initialisé : {self.client}")
            return self.client
        except Exception as e:
            logger.error(f"❌ Erreur de création du cluster Dask : {e}")
            return None

    def run(self, func, *args, **kwargs):
        """
        Exécute une fonction Dask distribuée et retourne son résultat.
        """
        if not self.client:
            logger.error("❌ Aucun client Dask actif.")
            return None
        try:
            future = self.client.submit(func, *args, **kwargs)
            result = future.result()
            logger.info("✅ Tâche Dask exécutée avec succès.")
            return result
        except Exception as e:
            logger.error(f"❌ Erreur lors de l’exécution Dask : {e}")
            return None

    def close(self):
        if self.client:
            self.client.close()
            logger.info("🛑 Client Dask arrêté proprement.")
