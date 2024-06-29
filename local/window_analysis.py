import os
from datetime import datetime
import pandas as pd

from local.libs.schedule_splitter import ScheduleSplitter
from local.libs.window_xer import WindowAnalyzer
from xerparser import Xer

date_format = "%Y-%m-%d %H:%M"
asbuilt_file = r"asbuilt.xer"

# define the start and end dates for the filter
start_date = '2022-11-12'  # example start date
end_date = '2022-11-30'  # example end date
split_date = end_date


def main():
    file = asbuilt_file
    with open(file, encoding=Xer.CODEC, errors="ignore") as f:
        file_contents = f.read()
    xer = Xer(file_contents)

    start_folder_path = os.path.join(os.getcwd(), "reports")
    end_folder_path = os.path.join(os.getcwd(), "reports")

    w = WindowAnalyzer(xer, start_folder_path, end_folder_path)
    w.generate_window_data_and_progress(start_date, end_date)


if __name__ == "__main__":
    main()
