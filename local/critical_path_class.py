import networkx as nx
import pandas as pd

class CriticalPathCalculator:
    def __init__(self, tasks_df, taskpred_df):
        self.tasks_df = tasks_df
        self.taskpred_df = taskpred_df
        self.G = nx.DiGraph()

    def build_graph(self):
        # Add nodes to the graph
        for _, row in self.tasks_df.iterrows():
            task_id = row['task_id']
            duration = pd.to_numeric(row['target_drtn_hr_cnt'], errors='coerce') / 8  # Convert hours to days
            self.G.add_node(task_id, duration=duration)

        # Add edges to the graph based on the predecessor relationships
        for _, row in self.taskpred_df.iterrows():
            pred_task_id = row['pred_task_id']
            task_id = row['task_id']
            pred_type = row['pred_type']
            lag_days = row['lag_days']

            if pred_type == 'PR_FS':
                self.G.add_edge(pred_task_id, task_id, weight=lag_days)
            elif pred_type == 'PR_SS':
                temp_node = f"temp_{pred_task_id}_{task_id}"
                self.G.add_node(temp_node, duration=0)
                self.G.add_edge(pred_task_id, temp_node, weight=0)
                self.G.add_edge(temp_node, task_id, weight=lag_days)
            elif pred_type == 'PR_FF':
                temp_node = f"temp_{pred_task_id}_{task_id}"
                self.G.add_node(temp_node, duration=0)
                pred_duration = self.G.nodes[pred_task_id].get('duration',
                                                               0)  # Get the predecessor duration, default to 0 if missing
                self.G.add_edge(pred_task_id, temp_node, weight=pred_duration)
                self.G.add_edge(temp_node, task_id, weight=lag_days)
            elif pred_type == 'PR_SF':
                temp_node = f"temp_{pred_task_id}_{task_id}"
                self.G.add_node(temp_node, duration=0)
                self.G.add_edge(pred_task_id, temp_node, weight=0)
                task_duration = self.G.nodes[task_id].get('duration',
                                                          0)  # Get the task duration, default to 0 if missing
                self.G.add_edge(temp_node, task_id, weight=task_duration + lag_days)
    def calculate_critical_path(self):
        # Calculate the critical path using NetworkX
        critical_path = nx.dag_longest_path(self.G)

        # Calculate the total duration of the critical path
        total_duration = 0
        for task_id in critical_path:
            total_duration += self.G.nodes[task_id]['duration']

        return critical_path, total_duration

    def run(self):
        self.build_graph()
        critical_path, total_duration = self.calculate_critical_path()
        critical_tasks_df = self.tasks_df[self.tasks_df['task_id'].isin(critical_path)]
        return critical_tasks_df, total_duration * 8  # Convert days back to hours