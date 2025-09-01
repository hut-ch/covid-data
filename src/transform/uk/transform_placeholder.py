"""UK Transformation process"""

from utils import get_logger, get_variable

logger = get_logger(__name__)


def process_uk(env_vars: dict | None):
    """placeholder function"""
    test = get_variable("CLEANSED_FOLDER", env_vars)
    logger.info("UK Transfomr TBD %s", test)
