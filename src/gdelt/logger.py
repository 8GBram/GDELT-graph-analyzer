import logging
import sys

logger = logging.getLogger("gdelt")
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(
    logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )
)

logger.addHandler(handler)