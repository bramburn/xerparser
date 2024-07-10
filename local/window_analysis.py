import os
from datetime import datetime
import pandas as pd

from local.libs.schedule_splitter import ScheduleSplitter
from local.libs.window_xer import WindowAnalyzer
from xerparser import Xer

date_format = "%Y-%m-%d %H:%M"
asbuilt_file = r"asbuilt3.xer"

# define the start and end dates for the filter
start_date = '2023-01-01'  # example start date
end_date = '2023-11-30'  # example end date
split_date = end_date
monitored_activities = ["HRPMMM1001",
                        "HRPMMM1002",
                        "HRPMMM1003",
                        "HRPMMM1005",
                        "HRPMMM1008",
                        "HRPMMM1010",
                        "HRPMMM1004",
                        "HRPMMM1007",
                        "HRPMMM1013",
                        "HRPMMM1012",
                        "HRPMMM1006",
                        "HRPMMM1009",
                        "HRPMMM1011",
                        "HRPMMM1014",
                        "HRVHMM1000",
                        "HRPMMM405",
                        "HRPMMM0001",
                        "HRPMMM395",
                        "HRPMMM0005",
                        "HRPMMM0015",
                        "HRPMMM0030",
                        "HRPMMM0035",
                        "HRPMMM6730",
                        "HRPMMM605" ]


def main():
    file = asbuilt_file
    with open(file, encoding=Xer.CODEC, errors="ignore") as f:
        file_contents = f.read()
    xer = Xer(file_contents)

    report_folder_path = os.path.join(os.getcwd(), "reports")

    w = WindowAnalyzer(xer, report_folder_path)
    w.set_monitored_tasks(monitored_activities)
    w.generate_window_data_and_progress(start_date, end_date)


if __name__ == "__main__":
    main()
