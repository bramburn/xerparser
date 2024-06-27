import os
from datetime import datetime
import pandas as pd

from local.libs.schedule_splitter import ScheduleSplitter
from xerparser import Xer

date_format = "%Y-%m-%d %H:%M"
asbuilt_file = r"asbuilt.xer"

# define the start and end dates for the filter
start_date = '2023-12-01 00:00:00'  # example start date
end_date = '2023-12-31 00:00:00'  # example end date
split_date = end_date

def main():
    file = asbuilt_file
    with open(file, encoding=Xer.CODEC, errors="ignore") as f:
        file_contents = f.read()
    xer = Xer(file_contents)

    # Create a ScheduleSplitter instance
    splitter = ScheduleSplitter(xer)
    modified_xer1, filtered_tasks1 = splitter.process_data("2023-01-01", "2023-03-31", "2023-03-15")
    splitter.generate_xer("progress1.xer")
    splitter.generate_markdown_report("report1.md")




if __name__ == "__main__":
    main()