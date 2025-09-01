"""Manage UK specific Dimensions"""

from utils import get_logger

logger = get_logger(__name__)


def maintain_uk_dims():
    "Run UK Dimension loads"

    logger.info("Starting UK Dimension Load")
