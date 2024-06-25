import networkx as nx
from datetime import datetime, timedelta
import pandas as pd


class TotalFloatCPMCalculator:
    def __init__(self, xer_object):
        self.xer = xer_object
        self.graph = nx.DiGraph()
        self.early_start = {}
        self.early_finish = {}
        self.late_start = {}
        self.late_finish = {}
        self.total_float = {}
        self.critical_path = []

    def build_graph(self):
        for _, task in self.xer.task_df.iterrows():
            try:
                duration = pd.to_timedelta(pd.to_numeric(task['target_drtn_hr_cnt']), unit='h').days
            except ValueError:
                # Handle cases where conversion fails (e.g., non-numeric strings)
                print(f"Warning: Invalid duration for task {task['task_id']}: {task['target_drtn_hr_cnt']}")
                duration = 0
            self.graph.add_node(task['task_id'], duration=duration)

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
                    self.early_finish[p] + timedelta(days=self.graph[p][node]['lag'])
                    for p in predecessors
                )
            self.early_finish[node] = self.early_start[node] + timedelta(days=self.graph.nodes[node]['duration'])

    def backward_pass(self):
        project_end = max(self.early_finish.values())
        for node in reversed(list(nx.topological_sort(self.graph))):
            successors = list(self.graph.successors(node))
            if not successors:
                self.late_finish[node] = project_end
            else:
                self.late_finish[node] = min(
                    self.late_start[s] - timedelta(days=self.graph[node][s]['lag'])
                    for s in successors
                )
            self.late_start[node] = self.late_finish[node] - timedelta(days=self.graph.nodes[node]['duration'])

    def calculate_total_float(self):
        for node in self.graph.nodes:
            self.total_float[node] = (self.late_start[node] - self.early_start[node]).days

    def determine_critical_path(self, float_threshold=0):
        self.critical_path = [node for node, tf in self.total_float.items() if tf <= float_threshold]

    def calculate_critical_path(self):
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