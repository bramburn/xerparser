from local.libs.total_float_method import TotalFloatCPMCalculator
from xerparser import Xer
import logging


def main():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    file = r"updated.xer"
    try:
        with open(file, encoding=Xer.CODEC, errors="ignore") as f:
            file_contents = f.read()

        xer = Xer(file_contents)

        # Create an instance of the TotalFloatCPMCalculator and run the calculations
        calculator = TotalFloatCPMCalculator(xer)
        critical_path = calculator.calculate_critical_path()
        calculator.update_task_df()

        # Print the results
        calculator.print_results()

        # Optionally, you can add more detailed output here
        logger.info(f"Number of tasks on the critical path: {len(critical_path)}")
        logger.info(f"Project duration: {calculator.get_project_duration()} days")

    except FileNotFoundError:
        logger.error(f"File not found: {file}")
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    main()