import os

# ===========================================================
#  RÉPERTOIRE RACINE DU PROJET
# ===========================================================
# Permet de construire les chemins relatifs correctement,
# même si le script est lancé depuis un autre répertoire.
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ===========================================================
#  KAFKA
# ===========================================================
KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]
KAFKA_TOPIC = "flights"

# Taille du batch de messages à traiter avec Dask
KAFKA_BATCH_SIZE = 100

# Temps d’attente (en secondes) avant de forcer le traitement
KAFKA_BATCH_TIMEOUT = 30

# ===========================================================
#  DASK
# ===========================================================
# Nombre de workers pour le cluster local
DASK_N_WORKERS = 4

# Niveau de parallélisme (threads/worker)
DASK_THREADS_PER_WORKER = 1

# Temps maximal d’inactivité avant arrêt automatique
DASK_IDLE_TIMEOUT = "60s"

# ===========================================================
#  MONGODB
# ===========================================================
MONGO_URI = "mongodb://localhost:27017"
MONGO_DB = "flights"

# ===========================================================
#  LOGGING
# ===========================================================
# Le fichier de log sera stocké dans la racine du dossier etape2_Dask
LOG_FILE = os.path.join(BASE_DIR, "dask_consumer.log")

# Niveau de verbosité par défaut
LOG_LEVEL = "INFO"

# ===========================================================
#  OPTIONS GÉNÉRALES
# ===========================================================
# Active le mode "simulation" (ne stocke rien dans Mongo)
DEBUG_MODE = False

# Si True → affiche les DataFrames Dask intermédiaires
VERBOSE_BATCH_OUTPUT = False
