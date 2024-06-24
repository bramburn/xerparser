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

            if pred_task_id not in self.G.nodes or task_id not in self.G.nodes:
                continue

            pred_type = row['pred_type']
            lag_days = row['lag_days']

            self.G.add_edge(pred_task_id, task_id, weight=lag_days, pred_type=pred_type)

    def calculate_critical_path(self):
        # Topological sort of the graph
        topological_order = list(nx.topological_sort(self.G))

        # Calculate early start and early finish times
        early_start = {node: 0 for node in self.G.nodes()}
        early_finish = {node: 0 for node in self.G.nodes()}

        for node in topological_order:
            predecessors = list(self.G.predecessors(node))
            if predecessors:
                for pred in predecessors:
                    edge_data = self.G.get_edge_data(pred, node)
                    pred_type = edge_data['pred_type']
                    lag = edge_data['weight']

                    if pred_type == 'PR_FS':
                        early_start[node] = max(early_start[node], early_finish[pred] + lag)
                    elif pred_type == 'PR_SS':
                        early_start[node] = max(early_start[node], early_start[pred] + lag)
                    elif pred_type == 'PR_FF':
                        early_finish[node] = max(early_finish[node], early_finish[pred] + lag)
                    elif pred_type == 'PR_SF':
                        early_finish[node] = max(early_finish[node], early_start[pred] + lag)

            duration = self.G.nodes[node]['duration']
            early_finish[node] = max(early_finish[node], early_start[node] + duration)

        # Find the project end time
        project_end = max(early_finish.values())

        # Calculate late start and late finish times
        late_start = {node: project_end for node in self.G.nodes()}
        late_finish = {node: project_end for node in self.G.nodes()}

        for node in reversed(topological_order):
            successors = list(self.G.successors(node))
            duration = self.G.nodes[node]['duration']

            if successors:
                for succ in successors:
                    edge_data = self.G.get_edge_data(node, succ)
                    pred_type = edge_data['pred_type']
                    lag = edge_data['weight']

                    if pred_type == 'PR_FS':
                        late_finish[node] = min(late_finish[node], late_start[succ] - lag)
                    elif pred_type == 'PR_SS':
                        late_start[node] = min(late_start[node], late_start[succ] - lag)
                    elif pred_type == 'PR_FF':
                        late_finish[node] = min(late_finish[node], late_finish[succ] - lag)
                    elif pred_type == 'PR_SF':
                        late_start[node] = min(late_start[node], late_finish[succ] - lag)

            late_start[node] = min(late_start[node], late_finish[node] - duration)

        # Identify critical path
        critical_path = []
        current = max(late_finish, key=late_finish.get)
        while current is not None:
            critical_path.append(current)
            predecessors = list(self.G.predecessors(current))
            current = None
            for pred in predecessors:
                if early_start[pred] == late_start[pred]:
                    current = pred
                    break
        critical_path.reverse()

        return critical_path, project_end, early_start, early_finish, late_start, late_finish

    def get_critical_path_relationships(self, critical_path):
        relationships = []
        for i, curr_task in enumerate(critical_path):
            for j in range(i + 1, len(critical_path)):
                next_task = critical_path[j]
                edge_data = self.G.get_edge_data(curr_task, next_task)
                if edge_data:
                    relationships.append({
                        'from_task': curr_task,
                        'to_task': next_task,
                        'relationship': edge_data['pred_type'],
                        'lag': edge_data['weight']
                    })
                    break  # We've found the next relationship, so we can move to the next task
        return pd.DataFrame(relationships)

    def validate_critical_path(self, critical_path, early_start, late_start):
        for task in critical_path:
            total_float = late_start[task] - early_start[task]
            if abs(total_float) > 1e-6:  # Use small epsilon for float comparison
                print(f"Warning: Task {task} on critical path has non-zero float: {total_float}")

    def validate_path_duration(self, critical_path):
        critical_duration = sum(self.G.nodes[task]['duration'] for task in critical_path)
        all_paths = list(nx.all_simple_paths(self.G, source=critical_path[0], target=critical_path[-1]))
        for path in all_paths:
            path_duration = sum(self.G.nodes[task]['duration'] for task in path)
            if path_duration > critical_duration:
                print(f"Warning: Found longer path than critical path: {path}")

    def find_all_critical_paths(self, early_start, late_start):
        start_nodes = [node for node in self.G.nodes() if self.G.in_degree(node) == 0]
        end_nodes = [node for node in self.G.nodes() if self.G.out_degree(node) == 0]
        all_critical_paths = []
        for start in start_nodes:
            for end in end_nodes:
                paths = list(nx.all_simple_paths(self.G, start, end))
                for path in paths:
                    if all(abs(late_start[task] - early_start[task]) < 1e-6 for task in path):
                        all_critical_paths.append(path)
        return all_critical_paths

    # def visualize_network(self, critical_path):
    #     pos = nx.spring_layout(self.G)
    #     nx.draw(self.G, pos, with_labels=True, node_color='lightblue', node_size=500, font_size=10, font_weight='bold')
    #     nx.draw_networkx_nodes(self.G, pos, nodelist=critical_path, node_color='red', node_size=600)
    #     edge_labels = nx.get_edge_attributes(self.G, 'pred_type')
    #     nx.draw_networkx_edge_labels(self.G, pos, edge_labels=edge_labels)
    #     plt.title("Project Network (Critical Path in Red)")
    #     plt.axis('off')
    #     plt.show()


    def validate_path_continuity(self, critical_path):
        for i in range(len(critical_path) - 1):
            if not self.G.has_edge(critical_path[i], critical_path[i + 1]):
                print(f"Warning: No direct link between {critical_path[i]} and {critical_path[i + 1]}")


    def run(self):
        self.build_graph()
        critical_path, total_duration, early_start, early_finish, late_start, late_finish = self.calculate_critical_path()

        self.validate_critical_path(critical_path, early_start, late_start)
        self.validate_path_duration(critical_path)
        self.validate_path_continuity(critical_path)
        all_critical_paths = self.find_all_critical_paths(early_start, late_start)
        if len(all_critical_paths) > 1:
            print(f"Found {len(all_critical_paths)} critical paths")

        # Ensure all tasks in the critical path are included
        critical_tasks_df = self.tasks_df[self.tasks_df['task_id'].isin(critical_path)].copy()

        # If some tasks are missing, it might be due to temporary nodes. Let's add them manually.
        missing_tasks = set(critical_path) - set(critical_tasks_df['task_id'])
        for task_id in missing_tasks:
            temp_task = pd.DataFrame({
                'task_id': [task_id],
                'task_code': [f'TEMP_{task_id}'],
                'task_name': [f'Temporary Task {task_id}'],
                'target_drtn_hr_cnt': [0],
                'proj_id': ['TEMP']
            })
            critical_tasks_df = pd.concat([critical_tasks_df, temp_task], ignore_index=True)

        critical_tasks_df['order'] = critical_tasks_df['task_id'].map({task: i for i, task in enumerate(critical_path)})
        critical_tasks_df = critical_tasks_df.sort_values('order')

        critical_tasks_df['early_start'] = critical_tasks_df['task_id'].map(early_start)
        critical_tasks_df['early_finish'] = critical_tasks_df['task_id'].map(early_finish)
        critical_tasks_df['late_start'] = critical_tasks_df['task_id'].map(late_start)
        critical_tasks_df['late_finish'] = critical_tasks_df['task_id'].map(late_finish)

        critical_relationships_df = self.get_critical_path_relationships(critical_path)

        return critical_tasks_df, critical_relationships_df, total_duration * 8  # Convert days back to hours