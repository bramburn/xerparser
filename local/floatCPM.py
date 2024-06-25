from local.libs.total_float_method import TotalFloatCPMCalculator
from xerparser import Xer


def main():
    file = r"updated.xer"
    with open(file, encoding=Xer.CODEC, errors="ignore") as f:
        file_contents = f.read()
    xer = Xer(file_contents)
    #
    # Assuming you have created an Xer object named 'xer'
    calculator = TotalFloatCPMCalculator(xer)
    critical_path = calculator.calculate_critical_path()
    calculator.update_task_df()
    calculator.print_results()

if __name__ == "__main__":
    main()
