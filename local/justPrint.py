import numpy as np
from datetime import datetime, timedelta

import pandas as pd

from xerparser.src.xer import Xer


def main():
    file = r"baseline.xer"
    with open(file, encoding=Xer.CODEC, errors="ignore") as f:
        file_contents = f.read()
    xer = Xer(file_contents)

    if xer.project_df is not None:
        print("Project IDs:")
        for proj_id in xer.project_df['proj_id']:
            print(proj_id)
    else:
        print("No project data found in the XER file.")

if __name__ == "__main__":
    main()