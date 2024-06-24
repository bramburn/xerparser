import numpy as np
from datetime import datetime, timedelta
from xerparser import Xer

def main():
    file = r"updated.xer"
    with open(file, encoding=Xer.CODEC, errors="ignore") as f:
        file_contents = f.read()
    xer = Xer(file_contents)
    projects = xer.projects
    start_date = datetime(2022, 11, 1)
    end_date = datetime(2023, 1, 8)

    for p in projects.values():
        print(f"Project: {p.name}")
        tasks = [task for task in p.tasks if start_date <= task.start <= end_date or start_date <= task.finish <= end_date]
        if tasks:
            print("Tasks:")
            for task in tasks:
                print(f"  - {task.task_code} - {task.name}")
                print(f"    Start: {task.start.strftime('%Y-%m-%d')}, Finish: {task.finish.strftime('%Y-%m-%d')}, status = {task.status.value}, % complete {task.phys_complete_pct:.2f}%")
        else:
            print("No tasks found in the given date range.")
        print()

if __name__ == "__main__":
    main()