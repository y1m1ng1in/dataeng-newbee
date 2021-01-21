import glob, os
from datetime import date
from constants import breadcrumbs_dir


def find_data(dir=breadcrumbs_dir):
    os.chdir(dir)
    file = glob.glob("{}.json".format(str(date.today())))
    return file
