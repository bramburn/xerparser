# total_float_method.py
import logging
import networkx as nx
import pandas as pd
from local.libs.calendar_parser import CalendarParser
from local.libs.working_day_calculator import WorkingDayCalculator


class TotalFloatCPMCalculator:

    def __init__(self, xer_object):
        self.logger = logging.getLogger('TotalFloatCPMCalculator')
        self.logger.setLevel(logging.INFO)
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

    def apply_activity_constraints(self, node, is_forward_pass=True):
        cstr_type = self.graph.nodes[node]['cstr_type']
        cstr_date = self.graph.nodes[node]['cstr_date']
        cstr_type2 = self.graph.nodes[node]['cstr_type2']
        cstr_date2 = self.graph.nodes[node]['cstr_date2']

        early_start = self.early_start[node]
        late_finish = self.late_finish.get(node, None)  # Use .get() to avoid KeyError

        def apply_constraint(constraint_type, constraint_date, early_start, late_finish):
            if pd.isna(constraint_date):
                return early_start, late_finish

            constraint_date = pd.to_datetime(constraint_date).date()

            if constraint_type == 'CS_ALAP':
                pass  # No action needed for As Late as Possible
            elif constraint_type in ['CS_MEO', 'CS_MANDFIN']:
                if late_finish is not None:
                    late_finish = constraint_date
            elif constraint_type == 'CS_MEOA':
                if late_finish is not None:
                    late_finish = max(late_finish, constraint_date)
            elif constraint_type == 'CS_MEOB':
                if late_finish is not None:
                    late_finish = min(late_finish, constraint_date)
            elif constraint_type in ['CS_MANDSTART', 'CS_MSO']:
                early_start = constraint_date
            elif constraint_type == 'CS_MSOA':
                early_start = max(early_start, constraint_date)
            elif constraint_type == 'CS_MSOB':
                early_start = min(early_start, constraint_date)

            return early_start, late_finish

        # Apply primary constraint
        early_start, late_finish = apply_constraint(cstr_type, cstr_date, early_start, late_finish)

        # Apply secondary constraint
        early_start, late_finish = apply_constraint(cstr_type2, cstr_date2, early_start, late_finish)

        # Check for conflicts only if we have both early_start and late_finish
        if early_start is not None and late_finish is not None and early_start > late_finish:
            self.logger.warning(f"Conflict detected for task {node}: Early start is later than late finish")

        # Check if constraint violates predecessor relationships
        if is_forward_pass:
            for pred in self.graph.predecessors(node):
                if early_start < self.early_finish[pred]:
                    self.logger.warning(f"Constraint for task {node} violates predecessor {pred} relationship")

        return early_start, late_finish
    def set_workday_df(self, workday):
        self.workdays_df = workday.copy()

    def set_exception_df(self, exception):
        self.exceptions_df = exception.copy()

    def build_graph(self):
        for _, task in self.xer.task_df.iterrows():
            try:
                duration = pd.to_timedelta(pd.to_numeric(task['target_drtn_hr_cnt']), unit='h')
            except ValueError:
                print(f"Warning: Invalid duration for task {task['task_id']}: {task['target_drtn_hr_cnt']}")
                duration = pd.Timedelta(0, unit='h')
            self.graph.add_node(task['task_id'], duration=duration.days, calendar_id=task['clndr_id'],
                                cstr_type=task['cstr_type'], cstr_date=task['cstr_date'],
                                cstr_type2=task['cstr_type2'], cstr_date2=task['cstr_date2'])

        for _, pred in self.xer.taskpred_df.iterrows():
            try:
                lag = pd.to_timedelta(pd.to_numeric(pred['lag_hr_cnt']), unit='h')
            except ValueError:
                print(
                    f"Warning: Invalid lag for relationship {pred['pred_task_id']} -> {pred['task_id']}: {pred['lag_hr_cnt']}")
                lag = pd.Timedelta(0, unit='h')
            self.graph.add_edge(pred['pred_task_id'], pred['task_id'], lag=lag, taskpred_type=pred['pred_type'])

    def forward_pass(self):
        project_start = pd.to_datetime(self.xer.project_df['plan_start_date'].iloc[0])
        self.logger.info("Starting forward pass")

        # Initialize early start times for all nodes
        for node in self.graph.nodes:
            self.early_start[node] = None

        for node in nx.topological_sort(self.graph):
            if self.early_start[node] is None:
                self.early_start[node] = project_start

            predecessors = list(self.graph.predecessors(node))
            if not predecessors:
                self.early_start[node] = project_start
            else:
                es_list = []
                ef_list = []
                for pred in predecessors:
                    taskpred_type = self.graph[pred][node]['taskpred_type']
                    if taskpred_type == 'PR_FS':  # FS: Finish-to-Start
                        es_list.append(self.early_finish[pred] + self.graph[pred][node]['lag'])
                    elif taskpred_type == 'PR_SS':  # SS: Start-to-Start
                        es_list.append(self.early_start[pred] + self.graph[pred][node]['lag'])
                    elif taskpred_type == 'PR_SF':  # SF: Start-to-Finish
                        ef_list.append(self.early_start[pred] + self.graph[pred][node]['lag'])
                    elif taskpred_type == 'PR_FF':  # FF: Finish-to-Finish
                        ef_list.append(self.early_finish[pred] + self.graph[pred][node]['lag'])
                if es_list:
                    self.early_start[node] = max(es_list)
                if ef_list:
                    self.early_finish[node] = max(ef_list)
            # Apply constraints after calculating early start and before calculating early finish
            self.early_start[node], _ = self.apply_activity_constraints(node)
            # Calculate early finish based on early start and duration
            self.early_finish[node] = self.working_day_calculator.add_working_days(
                self.early_start[node],
                self.graph.nodes[node]['duration'],
                self.graph.nodes[node]['calendar_id']
            )
        self.logger.info("Forward pass completed")

    def backward_pass(self):
        project_end = max(self.early_finish.values())
        self.logger.info("Starting backward pass")

        # Initialize late finish times for all nodes
        for node in self.graph.nodes:
            self.late_finish[node] = None

        # Set the late finish for nodes without successors
        for node in self.graph.nodes:
            if not list(self.graph.successors(node)):
                self.late_finish[node] = project_end

                # Now proceed with the rest of the backward
                # Now proceed with the rest of the backward pass
                for node in reversed(list(nx.topological_sort(self.graph))):
                    if self.late_finish[node] is None:
                        successors = list(self.graph.successors(node))
                        lf_list = []
                        for succ in successors:
                            taskpred_type = self.graph[node][succ]['taskpred_type']
                            if taskpred_type == 'PR_FS':  # FS: Finish-to-Start
                                lf_list.append(self.late_start[succ] - self.graph[node][succ]['lag'])
                            elif taskpred_type == 'PR_SS':  # SS: Start-to-Start
                                lf_list.append(self.late_start[succ] - self.graph[node][succ]['lag'])
                            elif taskpred_type == 'PR_SF':  # SF: Start-to-Finish
                                lf_list.append(self.late_finish[succ] - self.graph[node][succ]['lag'])
                            elif taskpred_type == 'PR_FF':  # FF: Finish-to-Finish
                                lf_list.append(self.late_finish[succ] - self.graph[node][succ]['lag'])
                        if lf_list:
                            self.late_finish[node] = min(lf_list)
                        else:
                            self.late_finish[node] = project_end
                    # Apply constraints after calculating late finish and before calculating late start
                    _, self.late_finish[node] = self.apply_activity_constraints(node)
                    self.late_start[node] = self.working_day_calculator.add_working_days(
                        self.late_finish[node],
                        -self.graph.nodes[node]['duration'],
                        self.graph.nodes[node]['calendar_id']
                    )
                self.logger.info("Backward pass completed")

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

    def update_task_df(self):
        self.xer.task_df['early_start'] = self.xer.task_df['task_id'].map(self.early_start)
        self.xer.task_df['early_finish'] = self.xer.task_df['task_id'].map(self.early_finish)
        self.xer.task_df['late_start'] = self.xer.task_df['task_id'].map(self.late_start)
        self.xer.task_df['late_finish'] = self.xer.task_df['task_id'].map(self.late_finish)
        self.xer.task_df['total_float'] = self.xer.task_df['task_id'].map(self.total_float)
        self.xer.task_df['is_critical'] = self.xer.task_df['task_id'].isin(self.critical_path)

    def get_project_duration(self):
        if not self.critical_path:
            self.logger.error("Critical path is empty. Cannot determine project duration.")
            return pd.Timedelta(0)  # or raise an exception, or handle it as appropriate
        return self.late_finish[self.critical_path[-1]] - self.xer.project_df['plan_start_date'].iloc[0]

    def print_results(self):
        print("Task ID\tES\t\tEF\t\tLS\t\tLF\t\tTotal Float\tCritical")
        for _, task in self.xer.task_df.iterrows():
            print(f"{task['task_id']}\t{task['early_start']}\t{task['early_finish']}\t"
                  f"{task['late_start']}\t{task['late_finish']}\t{task['total_float']}\t{task['is_critical']}")
        project_duration = self.get_project_duration()
        if project_duration == pd.Timedelta(0):
            print("\nCritical Path: Not found")
            print("Project Duration: Unable to determine")
        else:
            print(f"\nCritical Path: {' -> '.join(map(str, self.critical_path))}")
            print(f"Project Duration: {project_duration.days} days")
