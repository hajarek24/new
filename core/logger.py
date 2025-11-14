"""
===========================================================
Logger centralisé
-----------------------------------------------------------
Crée un logger standard pour tous les modules du projet.
Les logs sont à la fois affichés dans la console et
enregistrés dans un fichier (défini dans config/settings.py).
===========================================================
"""

import logging
from logging.handlers import RotatingFileHandler
from config.settings import LOG_FILE, LOG_LEVEL


def get_logger(name: str) -> logging.Logger:
    """
    Crée et configure un logger réutilisable.
    """
    logger = logging.getLogger(name)

    if not logger.hasHandlers():  # éviter les doublons
        logger.setLevel(LOG_LEVEL.upper())

        # Format standard des logs
        formatter = logging.Formatter(
            fmt="[%(asctime)s] [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )

        # Handler console
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # Handler fichier (rotation automatique)
        file_handler = RotatingFileHandler(
            LOG_FILE, maxBytes=2_000_000, backupCount=5, encoding="utf-8"
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
