import pandas as pd
from xerparser import CODEC, Xer


class XerFileGenerator:
    def __init__(self, xer):
        self.xer = xer

    def create_modified_copy(self, split_date:pd.Timestamp):
        """
        Create a copy of the entire Xer object with modified task progress and updated recalc date.

        Args:
            split_date (datetime): The date to set as the new last_recalc_date and calculate progress against.

        Returns:
            Xer: A new Xer object with modified data.
        """
        new_xer = Xer.__new__(Xer)
        new_xer.tables = {key: df.copy() if isinstance(df, pd.DataFrame) else df for key, df in self.xer.tables.items()}
        new_xer.project_df = new_xer.tables.get('PROJECT', None)
        new_xer.task_df = new_xer.tables.get('TASK', None)
        new_xer.taskpred_df = new_xer.tables.get('TASKPRED', None)
        new_xer.projwbs_df = new_xer.tables.get('PROJWBS', None)
        new_xer.calendar_df = new_xer.tables.get('CALENDAR', None)
        new_xer.account_df = new_xer.tables.get('ACCOUNT', None)
        new_xer.workdays_df = self.xer.workdays_df.copy() if hasattr(self.xer, 'workdays_df') else pd.DataFrame()
        new_xer.exceptions_df = self.xer.exceptions_df.copy() if hasattr(self.xer, 'exceptions_df') else pd.DataFrame()

        # Update task progress
        if new_xer.task_df is not None:
            new_xer.task_df['progress'] = new_xer.task_df.apply(
                lambda row: ProgressCalculator.calculate_progress(row, split_date),
                axis=1
            )

            # Reset actual dates for tasks with zero progress
            zero_progress_mask = new_xer.task_df['progress'] == 0
            new_xer.task_df.loc[zero_progress_mask, 'act_start_date'] = pd.NaT
            new_xer.task_df.loc[zero_progress_mask, 'act_end_date'] = pd.NaT

        # Update last_recalc_date
        if new_xer.project_df is not None and 'last_recalc_date' in new_xer.project_df.columns:
            new_xer.project_df['last_recalc_date'] = split_date.strftime('%Y-%m-%d %H:%M')

        return new_xer
    def generate_xer_contents(self) -> str:
        """Generate the updated XER file contents from the modified DataFrames."""
        xer_contents = ""

        # Handle ERMHDR specially
        if 'ERMHDR' in self.xer.tables and not self.xer.tables['ERMHDR'].empty:
            # If ERMHDR exists, use it and replace empty values with ''
            ermhdr_row = self.xer.tables['ERMHDR'].iloc[0].fillna('')
            xer_contents += "ERMHDR\t" + "\t".join([str(x) for x in ermhdr_row]) + "\n"
        else:
            # If ERMHDR is missing or empty, create a minimal header based on the provided format
            xer_contents += "ERMHDR\t19.0\t2023-04-14\tProject\tUSER\tUSERNAME\tdbxDatabaseNoName\tProject Management\tUSD\n"

        for table_name, df in self.xer.tables.items():
            if table_name != 'ERMHDR' and not df.empty:
                xer_contents += f"%T\t{table_name}\n"
                xer_contents += "%F\t" + "\t".join(df.columns) + "\n"
                for _, row in df.iterrows():
                    xer_contents += "%R\t" + "\t".join([self._format_value(x) for x in row]) + "\n"

        return xer_contents

    def _format_value(self, value):
        """Format values for XER output, handling datetime objects."""
        if pd.isna(value):
            return ""
        elif isinstance(value, pd.Timestamp):
            return value.strftime('%Y-%m-%d %H:%M')
        else:
            return str(value)

    def generate_xer_file(self, output_file: str, date_prefix: str = None):
        """Generate and save the XER file."""
        if date_prefix:
            new_filename = f"{date_prefix}_{output_file}"
        else:
            new_filename = output_file

        xer_contents = self.generate_xer_contents()
        with open(new_filename, 'w', encoding=CODEC) as f:
            f.write(xer_contents)
        print(f"Updated XER file exported to: {new_filename}")


class ProgressCalculator:
    @staticmethod
    def calculate_progress(row, split_date):
        if row['act_end_date'] <= split_date:
            return 1.0
        elif row['act_start_date'] > split_date:
            return 0.0
        elif pd.notnull(row['act_start_date']) and pd.notnull(row['act_end_date']):
            return (split_date - row['act_start_date']) / (row['act_end_date'] - row['act_start_date'])
        else:
            return 0.0