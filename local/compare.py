import numpy as np
from datetime import datetime, timedelta
from xerparser import Xer, CorruptXerFile


def compare_tasks(baseline_tasks, updated_tasks):
    renamed_tasks = []
    deleted_tasks = []
    added_tasks = []

    baseline_task_codes = [task.task_code for task in baseline_tasks]
    updated_task_codes = [task.task_code for task in updated_tasks]

    # Detect renamed tasks
    for baseline_task in baseline_tasks:
        if baseline_task.task_code in updated_task_codes:
            updated_task = next(task for task in updated_tasks if task.task_code == baseline_task.task_code)
            if baseline_task.name != updated_task.name:
                renamed_tasks.append((baseline_task, updated_task))

    # Detect deleted tasks
    deleted_tasks = [task for task in baseline_tasks if task.task_code not in updated_task_codes]

    # Detect added tasks
    added_tasks = [task for task in updated_tasks if task.task_code not in baseline_task_codes]

    return renamed_tasks, deleted_tasks, added_tasks

def main():
    baseline_file = r"baseline.xer"
    updated_file = r"updated.xer"

    try:
        with open(baseline_file, encoding=Xer.CODEC, errors="ignore") as f:
            baseline_contents = f.read()
    except FileNotFoundError:
        print(f"Error: {baseline_file} not found.")
        return

    try:
        with open(updated_file, encoding=Xer.CODEC, errors="ignore") as f:
            updated_contents = f.read()
    except FileNotFoundError:
        print(f"Error: {updated_file} not found.")
        return

    try:
        baseline_xer = Xer(baseline_contents)
        baseline_projects = baseline_xer.projects
    except CorruptXerFile as e:
        print(f"Error: {baseline_file} is corrupt. {e}")
        return

    try:
        updated_xer = Xer(updated_contents)
        updated_projects = updated_xer.projects
    except CorruptXerFile as e:
        print(f"Error: {updated_file} is corrupt. {e}")
        return

    if baseline_projects and updated_projects:
        baseline_project = next((p for p in baseline_projects.values()), None)
        updated_project = next((p for p in updated_projects.values()), None)

        if baseline_project and updated_project:
            renamed_tasks, deleted_tasks, added_tasks = compare_tasks(baseline_project.tasks, updated_project.tasks)

            print("Renamed Tasks:")
            for baseline_task, updated_task in renamed_tasks:
                print(f"  - {baseline_task.task_code}: {baseline_task.name} -> {updated_task.name}")

            print("\nDeleted Tasks:")
            for task in deleted_tasks:
                print(f"  - {task.task_code} - {task.name}")

            print("\nAdded Tasks:")
            for task in added_tasks:
                print(f"  - {task.task_code} - {task.name}")
        else:
            print("No projects found in the input files.")
    else:
        print("No projects found in the input files.")

if __name__ == "__main__":
    main()