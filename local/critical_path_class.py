from datetime import datetime, timedelta

import networkx as nx


class CriticalPathCalculator:
    def __init__(self, tasks_df, taskpred_df):
        self.tasks_df = tasks_df
        self.taskpred_df = taskpred_df
        self.G = nx.DiGraph()
        self.critical_path = None
        self.critical_tasks = None

    def build_graph(self):
        self.G.clear()
        for index, row in self.tasks_df.iterrows():
            self.G.add_node(row['task_id'],
                            name=row['task_name'],
                            total_duration=row['duration'],
                            completed_duration=row['progress'] * row['duration'],
                            remaining_duration=row['remaining_days'],
                            early_start=datetime(2023, 1, 1),  # Set an arbitrary start date
                            early_finish=None,
                            late_start=None,
                            late_finish=None)

        # Explicitly set 'early_start' for each node as a property of the node
        for node in self.G.nodes:
            if 'early_start' not in self.G.nodes[node]:
                self.G.nodes[node]['early_start'] = datetime(2023, 1, 1)

        for index, row in self.taskpred_df.iterrows():
            self.G.add_edge(row['pred_task_id'], row['task_id'],
                            lag=row['lag_days'] * 24)  # Lag between tasks in hours
    def calculate_early_dates(self):
        for node in nx.topological_sort(self.G):
            if self.G.nodes[node]['early_start'] is None:
                predecessors = list(self.G.predecessors(node))
                if predecessors:
                    self.G.nodes[node]['early_start'] = max([self.G.nodes[predecessor]['early_finish'] + self.G[predecessor][node]['lag'] for predecessor in predecessors])
                else:
                    self.G.nodes[node]['early_start'] = datetime(2023, 1, 1)  # Set an arbitrary start date
                if 'completed_duration' in self.G.nodes[node] and self.G.nodes[node]['completed_duration'] > 0:
                    self.G.nodes[node]['early_start'] = max(self.G.nodes[node]['early_start'], self.G.nodes[node]['early_start'] + timedelta(hours=self.G.nodes[node]['completed_duration'] / 24))
            self.G.nodes[node]['early_finish'] = self.G.nodes[node]['early_start'] + timedelta(hours=self.G.nodes[node]['total_duration'] / 24)

    def calculate_late_dates(self):
        for node in reversed(list(nx.topological_sort(self.G))):
            if self.G.nodes[node]['late_finish'] is None:
                successors = list(self.G.successors(node))
                if successors:
                    self.G.nodes[node]['late_finish'] = min([self.G.nodes[successor]['late_start'] - self.G[node][successor]['lag'] for successor in successors])
                else:
                    self.G.nodes[node]['late_finish'] = self.G.nodes[node]['early_finish']  # Set the late finish date to the early finish date if there are no successors
            self.G.nodes[node]['late_start'] = self.G.nodes[node]['late_finish'] - timedelta(hours=self.G.nodes[node]['total_duration'] / 24)

    def identify_critical_path(self):
        self.critical_path = []
        for node in self.G.nodes:
            if self.G.nodes[node]['early_start'] == self.G.nodes[node]['late_start']:
                self.critical_path.append(node)
        self.critical_tasks = self.tasks_df[self.tasks_df['task_id'].isin(self.critical_path)]

    def calculate_critical_path_duration(self):
        self.total_critical_path_duration = sum([self.G.nodes[node]['total_duration'] for node in self.critical_path])

    def run(self):
        self.build_graph()
        self.calculate_early_dates()
        self.calculate_late_dates()
        self.identify_critical_path()
        self.calculate_critical_path_duration()
        return self.critical_tasks, self.total_critical_path_duration

# Usage example:
# tasks_df = pd.read_csv('tasks.csv')
# taskpred_df = pd.read_csv('taskpred.csv')
#
# calc = CriticalPathCalculator(tasks_df, taskpred_df)
# critical_tasks, total_duration = calc.run()
# print("Critical tasks:")
# print(critical_tasks)
# print("Total duration of critical path:", total_duration, "hours")