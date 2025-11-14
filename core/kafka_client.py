"""
===========================================================
Kafka Client
-----------------------------------------------------------
Gère la connexion au broker Kafka et la lecture des messages
en mode "temps réel" (auto_offset_reset='latest').
===========================================================
"""

from kafka import KafkaConsumer
import json
from core.logger import get_logger
from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    KAFKA_BATCH_SIZE,
    KAFKA_BATCH_TIMEOUT
)

logger = get_logger("KafkaClient")


class KafkaClient:
    def __init__(self):
        try:
            logger.info(f"🔌 Connexion à Kafka ({KAFKA_BOOTSTRAP_SERVERS}) ...")
            self.consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="latest",  # démarre à la fin du topic
                enable_auto_commit=False
            )
            logger.info(f"✅ Connecté au topic Kafka '{KAFKA_TOPIC}'.")
        except Exception as e:
            logger.error(f"❌ Échec de connexion à Kafka : {e}")
            self.consumer = None

    def read_batch(self):
        """
        Lit un batch de messages depuis Kafka.
        Retourne une liste de dictionnaires.
        """
        if not self.consumer:
            logger.error("❌ Kafka consumer non initialisé.")
            return []

        messages = []
        try:
            for message in self.consumer:
                messages.append(message.value)
                if len(messages) >= KAFKA_BATCH_SIZE:
                    logger.info(f"📦 Batch de {len(messages)} messages prêt pour traitement Dask.")
                    return messages
        except Exception as e:
            logger.error(f"❌ Erreur lors de la lecture Kafka : {e}")
            return []

    def close(self):
        if self.consumer:
            self.consumer.close()
            logger.info("🛑 Kafka consumer arrêté proprement.")
