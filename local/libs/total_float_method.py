# total_float_method.py

import networkx as nx
import pandas as pd

from local.libs.calendar_parser import CalendarParser
from local.libs.working_day_calculator import WorkingDayCalculator


class TotalFloatCPMCalculator:
    def __init__(self, xer_object):
        self.working_day_calculator = None
        self.xer = xer_object
        self.workdays_df = pd.DataFrame(columns=['clndr_id', 'day', 'start_time', 'end_time'])
        self.exceptions_df = pd.DataFrame(columns=['clndr_id', 'exception_date', 'start_time', 'end_time'])
        self.graph = nx.DiGraph()
        self.early_start = {}
        self.early_finish = {}
        self.late_start = {}
        self.late_finish = {}
        self.total_float = {}
        self.critical_path = []

    def set_workday_df(self,workday):
        self.workdays_df = workday.copy()

    def set_exception_df(self, exception):
        self.exceptions_df = exception.copy()





    def build_graph(self):
        for _, task in self.xer.task_df.iterrows():
            try:
                duration = pd.to_timedelta(pd.to_numeric(task['target_drtn_hr_cnt']), unit='h').days
            except ValueError:
                print(f"Warning: Invalid duration for task {task['task_id']}: {task['target_drtn_hr_cnt']}")
                duration = 0
            self.graph.add_node(task['task_id'], duration=duration, calendar_id=task['clndr_id'])

        for _, pred in self.xer.taskpred_df.iterrows():
            try:
                lag = pd.to_timedelta(pd.to_numeric(pred['lag_hr_cnt']), unit='h').days
            except ValueError:
                # Handle cases where conversion fails
                print(
                    f"Warning: Invalid lag for relationship {pred['pred_task_id']} -> {pred['task_id']}: {pred['lag_hr_cnt']}")
                lag = 0
            self.graph.add_edge(pred['pred_task_id'], pred['task_id'], lag=lag)

    def forward_pass(self):
        project_start = pd.to_datetime(self.xer.project_df['plan_start_date'].iloc[0])
        for node in nx.topological_sort(self.graph):
            predecessors = list(self.graph.predecessors(node))
            if not predecessors:
                self.early_start[node] = project_start
            else:
                self.early_start[node] = max(
                    self.working_day_calculator.add_working_days(
                        self.early_finish[p],
                        self.graph[p][node]['lag'],
                        self.graph.nodes[node]['calendar_id']
                    ) or self.early_finish[p]  # Use early_finish[p] if add_working_days returns None
                    for p in predecessors
                )
            end_date = self.working_day_calculator.add_working_days(
                self.early_start[node],
                self.graph.nodes[node]['duration'],
                self.graph.nodes[node]['calendar_id']
            )
            self.early_finish[node] = end_date if end_date is not None else self.early_start[node]

    def backward_pass(self):
        project_end = max(self.early_finish.values())
        for node in reversed(list(nx.topological_sort(self.graph))):
            successors = list(self.graph.successors(node))
            if not successors:
                self.late_finish[node] = project_end
            else:
                late_finish_candidates = []
                for s in successors:
                    late_start_s = self.late_start.get(s)
                    if late_start_s is not None:
                        late_finish = self.working_day_calculator.add_working_days(
                            late_start_s,
                            -self.graph[node][s]['lag'],
                            self.graph.nodes[node]['calendar_id']
                        )
                        if late_finish is not None:
                            late_finish_candidates.append(late_finish)

                if late_finish_candidates:
                    self.late_finish[node] = min(late_finish_candidates)
                else:
                    # If no valid late finish can be calculated, use the project end date
                    self.late_finish[node] = project_end

            late_start = self.working_day_calculator.add_working_days(
                self.late_finish[node],
                -self.graph.nodes[node]['duration'],
                self.graph.nodes[node]['calendar_id']
            )
            self.late_start[node] = late_start if late_start is not None else self.late_finish[node]
    def calculate_total_float(self):
        for node in self.graph.nodes:
            self.total_float[node] = self.working_day_calculator.get_working_days_between(
                self.early_start[node],
                self.late_start[node],
                self.graph.nodes[node]['calendar_id']
            )
    def determine_critical_path(self, float_threshold=0):
        self.critical_path = [node for node, tf in self.total_float.items() if tf <= float_threshold]

    def calculate_critical_path(self):
        self.working_day_calculator = WorkingDayCalculator(self.workdays_df, self.exceptions_df)
        self.build_graph()
        self.forward_pass()
        self.backward_pass()
        self.calculate_total_float()
        self.determine_critical_path()
        return self.critical_path

    def get_project_duration(self):
        return max(self.early_finish.values()) - min(self.early_start.values())

    def update_task_df(self):
        self.xer.task_df['early_start'] = self.xer.task_df['task_id'].map(self.early_start)
        self.xer.task_df['early_finish'] = self.xer.task_df['task_id'].map(self.early_finish)
        self.xer.task_df['late_start'] = self.xer.task_df['task_id'].map(self.late_start)
        self.xer.task_df['late_finish'] = self.xer.task_df['task_id'].map(self.late_finish)
        self.xer.task_df['total_float'] = self.xer.task_df['task_id'].map(self.total_float)
        self.xer.task_df['is_critical'] = self.xer.task_df['task_id'].isin(self.critical_path)

    def print_results(self):
        print("Task ID\tES\t\tEF\t\tLS\t\tLF\t\tTotal Float\tCritical")
        for _, task in self.xer.task_df.iterrows():
            print(f"{task['task_code']}\t{task['task_name']}\t{task['early_start']}\t{task['early_finish']}\t"
                  f"{task['late_start']}\t{task['late_finish']}\t{task['total_float']}\t{task['is_critical']}")
        print(f"\nCritical Path: {' -> '.join(map(str, self.critical_path))}")
        print(f"Project Duration: {self.get_project_duration().days} days")
