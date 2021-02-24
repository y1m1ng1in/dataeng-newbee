import glob, os
from datetime import date
import logging
import sys
from constants import BREADCRUMBS_DIR, LOG_PATH


def find_data(day=str(date.today()), dir=BREADCRUMBS_DIR) -> list:
    os.chdir(dir)
    file = glob.glob("{}.json".format(day))
    return file[0]


def load_logger(type, if_file=True, day=str(date.today())):
    os.makedirs(LOG_PATH, exist_ok=True)
    log_path = os.path.join(LOG_PATH, "{}_{}.log".format(day, type))

    # set loggingfile
    logging.basicConfig(
        format="%(message)s",
        level=logging.INFO,
        handlers=[logging.StreamHandler(sys.stdout),],
    )
    logger = logging.getLogger()
    if if_file:
        logger.addHandler(logging.FileHandler(log_path))
