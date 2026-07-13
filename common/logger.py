import logging


def setup_logging(level="INFO"):
    # Set root logger to WARNING to suppress third-party debug logs
    logging.basicConfig(
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        level=logging.WARNING
    )
    # Set your own logger to the desired level
    logging.getLogger("myapp").setLevel(getattr(logging, level.upper(), "INFO"))


def print_info(msg, *args, **kwargs):
    logging.getLogger("myapp").info(msg, *args, **kwargs)


def print_debug(msg, *args, **kwargs):
    logging.getLogger("myapp").debug(msg, *args, **kwargs)


def print_warning(msg, *args, **kwargs):
    logging.getLogger("myapp").warning(msg, *args, **kwargs)


def print_error(msg, *args, **kwargs):
    logging.getLogger("myapp").error(msg, *args, **kwargs)


def setup_crt_logging(level="DEBUG"):
    """
    Configure AWS Common Runtime logging.

    Args:
        level: one of "DEBUG", "INFO", "WARNING", or "ERROR".

    Example:
        from common.logger import setup_crt_logging
        setup_crt_logging("DEBUG")
    """
    log_level = getattr(logging, level.upper(), logging.DEBUG)

    crt_loggers = [
        "awscrt",
        "s3transfer.crt",
        "s3transfer",
        "botocore",
        "boto3",
    ]

    for logger_name in crt_loggers:
        logger = logging.getLogger(logger_name)
        logger.setLevel(log_level)
        if not logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(
                logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")
            )
            logger.addHandler(handler)
        logger.propagate = False

    print_info(f"CRT logging enabled at {level} level for: {', '.join(crt_loggers)}")
