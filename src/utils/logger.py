import logging

def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        ch = logging.StreamHandler()
        fh = logging.FileHandler("option_analysis.log")
        fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        ch.setFormatter(fmt)
        fh.setFormatter(fmt)
        logger.addHandler(ch)
        logger.addHandler(fh)
    return logger
  
