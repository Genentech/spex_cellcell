# import time
# import numpy as np
import subprocess
import pandas as pd
import tempfile
import numpy as np
import logging
from glob import glob
import os
import shutil


def run(**kwargs):

    my_data = kwargs.get('cluster')
    row, col = my_data.shape
    row_names: list = []
    for i in range(col):
        if i == 1:
            row_names.append('coordinate_x'),
        elif i == 2:
            row_names.append('coordinate_y')
        elif i == col-1:
            row_names.append('cluster_id')
        else:
            row_names.append(f'{i}')
    pd_data = pd.DataFrame(my_data)
    pd_data.columns = row_names

    dir_name = tempfile.mkdtemp()
    temp = tempfile.mktemp(suffix='.csv', dir=dir_name)
    pd_data.to_csv(path_or_buf=temp)

    prefix = f"java -jar {os.path.join(os.path.dirname(__file__), 'CellCell-1.0-SNAPSHOT-all.jar')} {dir_name} "
    command = f'{prefix} {kwargs.get("epsilons")} {kwargs.get("nboots")} ' \
              f'{kwargs.get("threshold")} {kwargs.get("useRois")} {kwargs.get("mode")} '

    process = subprocess.run(
        command,
        shell=True,
        universal_newlines=True,
        capture_output=True,
        text=True,
    )
    logger = logging.getLogger('cell_cell')
    if process.stderr:
        logger.error(process.stderr)
    if process.stdout:
        logger.debug(process.stdout)

    if process.returncode:
        logger.error(f"return code: {process.returncode}")

    result: dict = {}

    if process.returncode == 0:
        pattern = os.path.join(dir_name, '*', '*.csv')

        for file in glob(pattern, recursive=True):
            if not os.path.isfile(file):
                continue
            else:
                folder_name = os.path.basename(os.path.dirname(file))
                result_data = np.genfromtxt(file, delimiter=',')
                result[folder_name] = result_data

    shutil.rmtree(dir_name, ignore_errors=True)
    return {"CellCell": result}

# debug section
# kwargs = {
#     "epsilons": 234,
#     "nboots": 100,
#     "threshold": 10,
#     "useRois": 0,
#     "mode": 0
# }
# debug section

# my_data = np.genfromtxt('data.csv', delimiter=',')
# kwargs['cluster'] = my_data

# print(run(**kwargs))

# debug section
