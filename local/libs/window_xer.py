from typing import Tuple, NamedTuple
import pandas as pd
import os
import logging
from xerparser import Xer
from local.libs.xer_file_creation import XerFileGenerator
from local.libs.total_float_method import TotalFloatCPMCalculator


class WindowXER(NamedTuple):
    xer: Xer
    critical_path: list
    file_path: str


class WindowAnalyzer:
    def __init__(self, xer, start_window_folder_path, end_window_folder_path):
        self.xer = xer
        self.start_window_xer_folder_path = start_window_folder_path
        self.end_window_xer_folder_path = end_window_folder_path
        self.xer_generator = XerFileGenerator(self.xer)

    def process_window(self, date: pd.Timestamp, is_end_window: bool) -> WindowXER:
        window_xer = self.xer_generator.create_modified_copy(date)

        calculator = TotalFloatCPMCalculator(window_xer)
        calculator.set_workday_df(window_xer.workday_df)
        calculator.set_exception_df(window_xer.exception_df)
        critical_path = calculator.calculate_critical_path()
        calculator.update_task_df()

        folder_path = self.end_window_xer_folder_path if is_end_window else self.start_window_xer_folder_path
        window_type = "end" if is_end_window else "start"
        file_name = os.path.join(folder_path, f"{date.strftime('%Y-%m-%d')}_{window_type}_window.xer")
        self.xer_generator.build_xer_file(window_xer, file_name)

        return WindowXER(window_xer, critical_path, file_name)

    def generate_markdown_report(self, window_xer: WindowXER, is_end_window: bool):
        # TODO: Implement markdown report generation
        pass

    def generate_window_data_and_progress(self, start_date: str, end_date: str) -> Tuple[WindowXER, WindowXER]:
        if self.start_window_xer_folder_path is None or self.end_window_xer_folder_path is None:
            logging.error("Both start and end window XER file paths must be set.")
            raise ValueError("Both start and end window XER file paths must be set.")

        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)

        end_window = self.process_window(end_date, is_end_window=True)
        start_window = self.process_window(start_date, is_end_window=False)

        self.generate_markdown_report(end_window, is_end_window=True)
        self.generate_markdown_report(start_window, is_end_window=False)

        # TODO: Compare the windows milestone and activities planned vs complete

        return start_window, end_window