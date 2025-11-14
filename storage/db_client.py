"""
===========================================================
MongoDB Client
-----------------------------------------------------------
Fournit une interface simple pour écrire les métriques
calculées par Dask dans la base MongoDB `flights`.
Les mises à jour se font en mode cumulatif (upsert + $inc).
===========================================================
"""

from pymongo import MongoClient
from core.logger import get_logger
from config.settings import MONGO_URI, MONGO_DB

logger = get_logger("MongoDBClient")


class MongoDBClient:
    def __init__(self):
        """Connexion à la base MongoDB."""
        try:
            self.client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
            self.db = self.client[MONGO_DB]
            self.client.server_info()  # Vérifie la connexion
            logger.info(f"✅ Connecté à MongoDB — base : {MONGO_DB}")
        except Exception as e:
            logger.error(f"❌ Erreur de connexion à MongoDB : {e}")
            self.client = None
            self.db = None

    # ============================================================
    #  MÉTRIQUES GLOBALES
    # ============================================================
    def update_stats_global(self, total, cancelled, delayed):
        if self.db is None:
            logger.error("❌ Connexion MongoDB absente.")
            return
        try:
            self.db["stats_global"].update_one(
                {"_id": "global"},
                {"$inc": {
                    "total_flights": total,
                    "cancelled": cancelled,
                    "delayed": delayed
                }},
                upsert=True
            )
            logger.info("✅ stats_global mise à jour avec succès.")
        except Exception as e:
            logger.error(f"❌ Erreur lors de la mise à jour des stats_global : {e}")

    # ============================================================
    #  ANNULATIONS (A, B, C, D)
    # ============================================================
    def update_cancellations(self, cancellations_dict: dict):
        if self.db is None:
            return
        try:
            for cause, count in cancellations_dict.items():
                self.db["cancellations"].update_one(
                    {"cause": cause},
                    {"$inc": {"count": count}},
                    upsert=True
                )
            logger.info("✅ cancellations mises à jour avec succès.")
        except Exception as e:
            logger.error(f"❌ Erreur update cancellations : {e}")

    # ============================================================
    #  JOUR DE LA SEMAINE
    # ============================================================
    def update_day_of_week(self, day_counts: dict):
        if self.db is None:
            return
        try:
            for day, count in day_counts.items():
                self.db["day_of_week"].update_one(
                    {"day": int(day)},
                    {"$inc": {"count": int(count)}},
                    upsert=True
                )
            logger.info("✅ day_of_week mis à jour avec succès.")
        except Exception as e:
            logger.error(f"❌ Erreur update day_of_week : {e}")

    # ============================================================
    #  ROUTES ORIGIN–DEST
    # ============================================================
    def update_routes(self, routes_list: list):
        if self.db is None:
            return
        try:
            for route in routes_list:
                origin = route.get("Origin") or route.get("origin")
                dest = route.get("Dest") or route.get("dest")
                flights = route.get("flights", 0)

                if origin and dest:
                    self.db["routes"].update_one(
                        {"origin": origin, "dest": dest},
                        {"$inc": {"flights": int(flights)}},
                        upsert=True
                    )
            logger.info("✅ routes mises à jour avec succès.")
        except Exception as e:
            logger.error(f"❌ Erreur update routes : {e}")

    # ============================================================
    #  COMPAGNIES AÉRIENNES
    # ============================================================
    def update_carriers(self, carriers_stats):
        if self.db is None:
            return
        try:
            # Si c’est une liste → transformer en dict attendu
            if isinstance(carriers_stats, list):
                carriers_dict = {
                    c["carrier"]: {
                        "total": c.get("total", 0),
                        "cancelled": c.get("cancelled", 0),
                        "delayed": c.get("delayed", 0)
                    } for c in carriers_stats
                }
            else:
                carriers_dict = carriers_stats

            for carrier, values in carriers_dict.items():
                self.db["carriers"].update_one(
                    {"carrier": carrier},
                    {"$inc": {
                        "total": values.get("total", 0),
                        "cancelled": values.get("cancelled", 0),
                        "delayed": values.get("delayed", 0)
                    }},
                    upsert=True
                )
            logger.info("✅ carriers mises à jour avec succès.")
        except Exception as e:
            logger.error(f"❌ Erreur update carriers : {e}")

    # ============================================================
    #  AÉROPORTS MENSUELS (Year, Month, Airport)
    # ============================================================
    def update_airports_monthly(self, airports_list: list):
        if self.db is None:
            return
        try:
            for a in airports_list:
                year = a.get("Year") or a.get("year")
                month = a.get("Month") or a.get("month")
                airport = a.get("Dest") or a.get("dest") or a.get("airport")
                arrivals = a.get("arrivals") or a.get("count", 0)

                if year and month and airport:
                    self.db["airports_monthly"].update_one(
                        {"year": int(year), "month": int(month), "airport": airport},
                        {"$inc": {"arrivals": int(arrivals)}},
                        upsert=True
                    )
            logger.info("✅ airports_monthly mis à jour avec succès.")
        except Exception as e:
            logger.error(f"❌ Erreur update airports_monthly : {e}")

    # ============================================================
    #  FERMETURE
    # ============================================================
    def close(self):
        if self.client:
            self.client.close()
            logger.info("🔒 Connexion MongoDB fermée proprement.")
