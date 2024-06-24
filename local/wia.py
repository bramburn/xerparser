import numpy as np
from datetime import datetime, timedelta
from xerparser import Xer


# get list of planned completions

# date of analysis 2022-01-09

#analysis windows
# 1. 2022-11-01 to 2024-01-01


#activities to monitor
# Activity ID
# HRPMMM605
# HRPMMM6730
# HRPMMM0035
# HRPMMM0030
# HRPMMM0015
# HRPMMM0005
# HRPMMM395
# HRPMMM0001
# HRPMMM405
# HRVHMM1000

# HRPMMM1014
# HRPMMM1011
# HRPMMM1009
# HRPMMM1006
# HRPMMM1012
# HRPMMM1013
# HRPMMM1007
# HRPMMM1004
# HRPMMM1010
# HRPMMM1008
# HRPMMM1005
# HRPMMM1003
# HRPMMM1002
# HRPMMM1001


def main():
    file = r"baseline.xer"
    with open(file, encoding=Xer.CODEC, errors="ignore") as f:
        file_contents = f.read()
    xer = Xer(file_contents)

    # Iterate through all the projects
    for _, project_row in xer.project_df.iterrows():
        print(f"Project Name: {project_row['proj_short_name']} uid: {project_row['proj_id']}")

if __name__ == "__main__":
    main()