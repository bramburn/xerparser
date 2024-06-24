import numpy as np
from datetime import datetime, timedelta

import pandas as pd

from xerparser.src.xer import Xer


def main():
    file = r"updated.xer"
    with open(file, encoding=Xer.CODEC, errors="ignore") as f:
        file_contents = f.read()
    xer = Xer(file_contents)

    # if version is 16> then we need to use a different mapping

    if xer.project_df is not None:
        print("Project IDs and Sum Data Dates:")
        for index, proj_id in enumerate(xer.project_df['proj_id']):
            # data date can be last_recalc_date
            sum_refresh_date = xer.project_df['last_recalc_date'][index]
            print(f"Project ID: {proj_id}, Sum Data Date: {sum_refresh_date}")
    else:
        print("No project data found in the XER file.")

if __name__ == "__main__":
    main()