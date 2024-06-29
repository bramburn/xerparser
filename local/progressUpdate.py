import os
from datetime import datetime
import pandas as pd
from xerparser import Xer
from local.libs.xer_file_creation import XerFileGenerator

def update_progress(input_file, progress_date, output_file):
    # Read the input XER file
    with open(input_file, encoding=Xer.CODEC, errors="ignore") as f:
        file_contents = f.read()
    xer = Xer(file_contents)

    # Convert progress_date to pandas Timestamp
    progress_date = pd.Timestamp(progress_date)

    # Create XerFileGenerator instance
    xer_generator = XerFileGenerator(xer)

    # Create modified copy with updated progress
    updated_xer = xer_generator.create_modified_copy(progress_date)

    # Generate the new XER file
    date_prefix = progress_date.strftime("%Y-%m-%d")
    output_filename = f"{date_prefix}_{output_file}"
    xer_generator.build_xer_file(updated_xer, output_filename)

    print(f"Updated XER file saved as: {output_filename}")

def main():
    input_file = "asbuilt.xer"
    progress_date = "2022-11-30"
    output_file = "updated.xer"

    update_progress(input_file, progress_date, output_file)

if __name__ == "__main__":
    main()