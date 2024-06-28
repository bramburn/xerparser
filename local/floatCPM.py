from local.libs.total_float_method import TotalFloatCPMCalculator
from xerparser import Xer
import logging


def main():
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger(__name__)
    # Create a file handler to write log messages to a file
    error_file_handler = logging.FileHandler('error.log')
    error_file_handler.setLevel(logging.ERROR)

    # Create a formatter and set the format for the log messages
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    error_file_handler.setFormatter(formatter)

    logger.addHandler(error_file_handler)

    file = r"baseline.xer"
    try:
        with open(file, encoding=Xer.CODEC, errors="ignore") as f:
            file_contents = f.read()

        xer = Xer(file_contents)


        # Create an instance of the TotalFloatCPMCalculator and run the calculations
        calculator = TotalFloatCPMCalculator(xer)
        calculator.set_workdays_df(xer.workday_df)
        calculator.set_exceptions_df(xer.exception_df)
        critical_path = calculator.calculate_critical_path()
        calculator.update_task_df()

        # Print the results
        calculator.print_results()

        # Optionally, you can add more detailed output here
        logger.info(f"Number of tasks on the critical path: {len(critical_path)}")
        logger.info(f"Project duration: {calculator.get_project_duration()} days")

    except FileNotFoundError:
        logger.error(f"File not found: {file}")



if __name__ == "__main__":
    main()