import logging
from typing import Sequence

logger = logging.getLogger(__name__)


def check_missing_cols(
    exp_cols: Sequence, returned_cols: Sequence, raise_error: bool = False
) -> None:
    """Convenience function to log if we are missing columns from a request.

    raises: ValueError if raise_error=True and if we are missing expected columns
    """
    missing_cols = [k for k in exp_cols if k not in returned_cols]

    if missing_cols:
        logger.error(f"Missing expectd columns {missing_cols}")

        if raise_error:
            raise ValueError(
                f"Missing required columns from response. Got {returned_cols}"
                f"Was expecting at least all of {exp_cols}"
            )


def safe_urljoin(host, endpoint):
    """Same as urljoin but leading/trailing '/' makes no difference"""
    return f"{host.rstrip('/')}/{endpoint.lstrip('/')}"
