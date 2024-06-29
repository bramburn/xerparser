import networkx as nx
import pandas as pd
from typing import List, Tuple, Dict
import math
from collections import Counter


class CriticalPathAnalyzer:
    def __init__(self, tasks_df: pd.DataFrame, taskpred_df: pd.DataFrame):
        self.tasks_df = tasks_df
        self.taskpred_df = taskpred_df
        self.G = nx.DiGraph()
        self.subgraphs = []
        self.critical_paths = []
        self.total_float = {}
        self.cycles = []
        self.virtual_start = "VIRTUAL_START"
        self.virtual_end = "VIRTUAL_END"

    def safe_float(self, value) -> float:
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0.0

    def build_graph(self):
        for _, task in self.tasks_df.iterrows():
            duration = self.safe_float(task['target_drtn_hr_cnt']) / 8  # Convert hours to days
            self.G.add_node(task['task_id'], duration=duration)

        for _, pred in self.taskpred_df.iterrows():
            weight = self.safe_float(pred['lag_days'])
            self.G.add_edge(pred['pred_task_id'], pred['task_id'],
                            weight=weight, pred_type=pred['pred_type'])

        # Add virtual start and end nodes
        start_nodes = [n for n in self.G.nodes() if self.G.in_degree(n) == 0]
        end_nodes = [n for n in self.G.nodes() if self.G.out_degree(n) == 0]

        self.G.add_node(self.virtual_start, duration=0)
        self.G.add_node(self.virtual_end, duration=0)

        for start_node in start_nodes:
            self.G.add_edge(self.virtual_start, start_node, weight=0, pred_type='PR_FS')

        for end_node in end_nodes:
            self.G.add_edge(end_node, self.virtual_end, weight=0, pred_type='PR_FS')

    def detect_cycles(self):
        try:
            cycles = list(nx.simple_cycles(self.G))
            if cycles:
                self.cycles = cycles
                print("Warning: Cycles detected in the project network.")
                for i, cycle in enumerate(cycles, 1):
                    print(f"Cycle {i}: {' -> '.join(map(str, cycle + [cycle[0]]))}")
            return bool(cycles)
        except nx.NetworkXNoCycle:
            return False

    def identify_cycles(self):
        try:
            cycles = list(nx.simple_cycles(self.G))
            if cycles:
                self.cycles = cycles
                print("Cycles detected in the project network:")
                for i, cycle in enumerate(cycles, 1):
                    print(f"Cycle {i}: {' -> '.join(map(str, cycle + [cycle[0]]))}")
            return cycles
        except nx.NetworkXNoCycle:
            print("No cycles detected in the project network.")
            return []

    def remove_cycles(self, method='auto', manual_breaks=None):
        cycles = self.identify_cycles()
        if not cycles:
            return

        if method == 'auto':
            print("Automatically removing cycles by breaking the longest duration relationship in each cycle.")
            for cycle in cycles:
                max_duration = -1
                edge_to_remove = None
                for i in range(len(cycle)):
                    u, v = cycle[i], cycle[(i + 1) % len(cycle)]
                    if self.G.has_edge(u, v):
                        duration = self.G.nodes[v].get('duration', 0)
                        if duration > max_duration:
                            max_duration = duration
                            edge_to_remove = (u, v)

                if edge_to_remove:
                    self.G.remove_edge(*edge_to_remove)
                    print(f"Removed edge {edge_to_remove} to break cycle.")

        elif method == 'manual':
            if not manual_breaks:
                print("Error: Manual breaks not provided.")
                return
            for edge in manual_breaks:
                if self.G.has_edge(*edge):
                    self.G.remove_edge(*edge)
                    print(f"Manually removed edge {edge} to break cycle.")
                else:
                    print(f"Warning: Edge {edge} not found in the graph.")

        else:
            print("Invalid method specified. Use 'auto' or 'manual'.")
            return

        remaining_cycles = self.identify_cycles()
        if remaining_cycles:
            print("Warning: Not all cycles could be removed.")
        else:
            print("All cycles have been successfully removed.")

    def remove_cycles(self):
        if not self.cycles:
            return

        print("Attempting to remove cycles by breaking the longest duration relationship in each cycle.")
        for cycle in self.cycles:
            max_duration = -1
            edge_to_remove = None
            for i in range(len(cycle)):
                u, v = cycle[i], cycle[(i + 1) % len(cycle)]
                if self.G.has_edge(u, v):
                    duration = self.G.nodes[v].get('duration', 0)
                    if duration > max_duration:
                        max_duration = duration
                        edge_to_remove = (u, v)

            if edge_to_remove:
                self.G.remove_edge(*edge_to_remove)
                print(f"Removed edge {edge_to_remove} to break cycle.")

        # Check if all cycles are removed
        if self.detect_cycles():
            print("Warning: Not all cycles could be removed automatically.")
        else:
            print("All cycles have been successfully removed.")

    def identify_subgraphs(self):
        undirected_G = self.G.to_undirected()
        self.subgraphs = list(nx.connected_components(undirected_G))

    def calculate_early_late_times(self, subgraph):
        early_start = {node: 0.0 for node in subgraph}
        early_finish = {node: 0.0 for node in subgraph}

        try:
            for node in nx.topological_sort(self.G.subgraph(subgraph)):
                max_early_start = 0.0
                max_early_finish = 0.0
                for pred in self.G.predecessors(node):
                    if pred in subgraph:
                        edge_data = self.G.edges[pred, node]
                        lag = self.safe_float(edge_data.get('weight', 0))
                        pred_type = edge_data.get('pred_type', 'PR_FS')

                        if pred_type == 'PR_FS':
                            max_early_start = max(max_early_start, early_finish[pred] + lag)
                        elif pred_type == 'PR_SS':
                            max_early_start = max(max_early_start, early_start[pred] + lag)
                        elif pred_type == 'PR_FF':
                            max_early_finish = max(max_early_finish, early_finish[pred] + lag)
                        elif pred_type == 'PR_SF':
                            max_early_finish = max(max_early_finish, early_start[pred] + lag)

                duration = self.safe_float(self.G.nodes[node].get('duration', 0))
                early_start[node] = max(max_early_start, max_early_finish - duration)
                early_finish[node] = max(max_early_finish, early_start[node] + duration)
        except nx.NetworkXUnfeasible:
            print("Error: The graph contains a cycle. Cannot perform topological sort.")
            return None, None, None, None, None

        subgraph_duration = early_finish.get(self.virtual_end, 0)

        late_finish = {node: subgraph_duration for node in subgraph}
        late_start = {node: subgraph_duration for node in subgraph}

        try:
            for node in reversed(list(nx.topological_sort(self.G.subgraph(subgraph)))):
                min_late_finish = subgraph_duration
                min_late_start = subgraph_duration
                for succ in self.G.successors(node):
                    if succ in subgraph:
                        edge_data = self.G.edges[node, succ]
                        lag = self.safe_float(edge_data.get('weight', 0))
                        pred_type = edge_data.get('pred_type', 'PR_FS')

                        if pred_type == 'PR_FS':
                            min_late_finish = min(min_late_finish, late_start[succ] - lag)
                        elif pred_type == 'PR_SS':
                            min_late_start = min(min_late_start, late_start[succ] - lag)
                        elif pred_type == 'PR_FF':
                            min_late_finish = min(min_late_finish, late_finish[succ] - lag)
                        elif pred_type == 'PR_SF':
                            min_late_start = min(min_late_start, late_finish[succ] - lag)

                duration = self.safe_float(self.G.nodes[node].get('duration', 0))
                late_finish[node] = min(min_late_finish, min_late_start + duration)
                late_start[node] = min(min_late_start, late_finish[node] - duration)
        except nx.NetworkXUnfeasible:
            print("Error: The graph contains a cycle. Cannot perform topological sort.")
            return None, None, None, None, None

        return early_start, early_finish, late_start, late_finish, subgraph_duration

    def calculate_float(self, early_start, late_start):
        return {node: late_start[node] - early_start[node] for node in early_start if
                node not in [self.virtual_start, self.virtual_end]}

    def identify_critical_path(self, subgraph, total_float):
        critical_paths = []
        for path in nx.all_simple_paths(self.G.subgraph(subgraph), self.virtual_start, self.virtual_end):
            if all(math.isclose(total_float.get(node, 0), 0, abs_tol=1e-6) for node in path if
                   node not in [self.virtual_start, self.virtual_end]):
                critical_paths.append([node for node in path if node not in [self.virtual_start, self.virtual_end]])
        return critical_paths

    def analyze(self) -> Tuple[List[List[int]], Dict[int, float], float]:
        self.build_graph()

        cycles = self.identify_cycles()
        if cycles:
            print("\nChoose how to handle cycles:")
            print("1. Automatically remove cycles")
            print("2. Manually specify edges to remove")
            print("3. Ignore cycles and continue analysis (not recommended)")
            choice = input("Enter your choice (1/2/3): ")

            if choice == '1':
                self.remove_cycles(method='auto')
            elif choice == '2':
                manual_breaks = []
                while True:
                    edge = input("Enter an edge to remove (format: 'node1,node2'), or press Enter to finish: ")
                    if not edge:
                        break
                    node1, node2 = edge.split(',')
                    manual_breaks.append((node1.strip(), node2.strip()))
                self.remove_cycles(method='manual', manual_breaks=manual_breaks)
            elif choice == '3':
                print("Continuing analysis with cycles present. Results may be unreliable.")
            else:
                print("Invalid choice. Continuing analysis with cycles present. Results may be unreliable.")

        self.identify_subgraphs()

        overall_critical_paths = []
        overall_total_float = {}
        overall_duration = 0.0

        for subgraph in self.subgraphs:
            if self.virtual_start in subgraph and self.virtual_end in subgraph:
                result = self.calculate_early_late_times(subgraph)
                if result is None:
                    continue  # Skip this subgraph if there was an error
                early_start, early_finish, late_start, late_finish, subgraph_duration = result
                subgraph_total_float = self.calculate_float(early_start, late_start)
                subgraph_critical_paths = self.identify_critical_path(subgraph, subgraph_total_float)

                overall_critical_paths.extend(subgraph_critical_paths)
                overall_total_float.update(subgraph_total_float)
                overall_duration = max(overall_duration, subgraph_duration)

        self.critical_paths = overall_critical_paths
        self.total_float = overall_total_float

        return overall_critical_paths, overall_total_float, overall_duration

    def get_critical_path_tasks(self) -> pd.DataFrame:
        if not self.critical_paths:
            return pd.DataFrame()

        critical_tasks = set()
        for path in self.critical_paths:
            critical_tasks.update(path)

        critical_tasks_df = self.tasks_df[self.tasks_df['task_id'].isin(critical_tasks)].copy()
        critical_tasks_df['total_float'] = critical_tasks_df['task_id'].map(self.total_float)

        return critical_tasks_df.sort_values('task_id')

    def print_critical_paths(self):
        if not self.critical_paths:
            print("No critical paths found.")
            return

        print(f"Number of critical paths: {len(self.critical_paths)}")
        for i, path in enumerate(self.critical_paths, 1):
            print(f"\nCritical Path {i}:")
            print(f"  Start: {path[0]}")
            print(f"  End: {path[-1]}")
            print(f"  Length: {len(path)} tasks")
            print(f"  Path: {' -> '.join(map(str, path))}")

    def print_subgraph_info(self):
        print(f"Number of subgraphs: {len(self.subgraphs)}")
        for i, subgraph in enumerate(self.subgraphs, 1):
            print(f"Subgraph {i}: {len(subgraph)} tasks")

    def print_multiple_start_end_info(self):
        start_nodes = [n for n in self.G.successors(self.virtual_start)]
        end_nodes = [n for n in self.G.predecessors(self.virtual_end)]

        print(f"Number of start points: {len(start_nodes)}")
        print("Start points:", ", ".join(map(str, start_nodes)))
        print(f"Number of end points: {len(end_nodes)}")
        print("End points:", ", ".join(map(str, end_nodes)))

    def print_detailed_critical_path_analysis(self):
        if not self.critical_paths:
            print("No critical paths found.")
            return

        print("\nDetailed Critical Path Analysis:")
        print(f"Number of critical paths: {len(self.critical_paths)}")

        start_nodes = set()
        end_nodes = set()

        for i, path in enumerate(self.critical_paths, 1):
            start_nodes.add(path[0])
            end_nodes.add(path[-1])
            print(f"\nCritical Path {i}:")
            print(f"  Start: {path[0]}")
            print(f"  End: {path[-1]}")
            print(f"  Length: {len(path)} tasks")
            print(f"  Duration: {sum(self.G.nodes[node]['duration'] for node in path):.2f} days")
            print(f"  Path: {' -> '.join(map(str, path))}")

        print(f"\nUnique start points in critical paths: {len(start_nodes)}")
        print("Start points:", ", ".join(map(str, start_nodes)))
        print(f"Unique end points in critical paths: {len(end_nodes)}")
        print("End points:", ", ".join(map(str, end_nodes)))

    def get_path_details(self, path):
        duration = sum(self.G.nodes[node]['duration'] for node in path)
        tasks = self.tasks_df[self.tasks_df['task_id'].isin(path)].copy()
        tasks['sequence'] = tasks['task_id'].map({node: i for i, node in enumerate(path)})
        tasks = tasks.sort_values('sequence')
        return {
            'start': path[0],
            'end': path[-1],
            'length': len(path),
            'duration': duration,
            'tasks': tasks
        }

    def get_all_critical_path_details(self):
        return [self.get_path_details(path) for path in self.critical_paths]

    def identify_most_critical_tasks(self, top_n=10):
        if not self.critical_paths:
            return pd.DataFrame()

        # Count task occurrences in critical paths
        task_counts = Counter(task for path in self.critical_paths for task in path)

        # Calculate task impact (duration * occurrence)
        task_impact = {task: count * self.G.nodes[task]['duration'] for task, count in task_counts.items()}

        # Sort tasks by impact
        sorted_tasks = sorted(task_impact.items(), key=lambda x: x[1], reverse=True)

        # Get top N tasks
        top_tasks = sorted_tasks[:top_n]

        # Create DataFrame with task details
        top_tasks_df = pd.DataFrame(top_tasks, columns=['task_id', 'impact_score'])
        top_tasks_df['occurrence_count'] = top_tasks_df['task_id'].map(task_counts)
        top_tasks_df['duration'] = top_tasks_df['task_id'].map(lambda x: self.G.nodes[x]['duration'])

        # Merge with task information
        task_info = self.tasks_df[['task_id', 'task_name']].set_index('task_id')
        top_tasks_df = top_tasks_df.join(task_info, on='task_id')

        # Calculate percentage of critical paths containing each task
        total_paths = len(self.critical_paths)
        top_tasks_df['critical_path_percentage'] = (top_tasks_df['occurrence_count'] / total_paths) * 100

        # Reorder columns
        column_order = ['task_id', 'task_name', 'occurrence_count', 'critical_path_percentage', 'duration',
                        'impact_score']
        top_tasks_df = top_tasks_df[column_order]

        return top_tasks_df.sort_values('impact_score', ascending=False).reset_index(drop=True)

    def print_most_critical_tasks(self, top_n=10):
        most_critical_tasks = self.identify_most_critical_tasks(top_n)

        if most_critical_tasks.empty:
            print("No critical tasks identified.")
            return

        print(f"\nTop {top_n} Most Critical Tasks:")
        print(most_critical_tasks.to_string(index=False))