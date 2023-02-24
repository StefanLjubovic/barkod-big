import logging
import sys

logging.basicConfig(
    format="[%(asctime)s] - [%(levelname)s] - [%(name)s] - %(message)s",
    level=logging.INFO,
    stream=sys.stdout,
)
