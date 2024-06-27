# total_float_method.py
import logging
import networkx as nx
import pandas as pd
from mdutils import MdUtils

from local.libs.calendar_parser import CalendarParser
from local.libs.working_day_calculator import WorkingDayCalculator
from pandas import Timestamp


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
        self.data_date = pd.to_datetime(self.xer.project_df['last_recalc_date'].iloc[0])

    def apply_activity_constraints(self, node, is_forward_pass=True):
        # Define valid constraint types
        VALID_CONSTRAINTS = {'CS_ALAP', 'CS_MEO', 'CS_MANDFIN', 'CS_MEOA', 'CS_MEOB',
                             'CS_MANDSTART', 'CS_MSO', 'CS_MSOA', 'CS_MSOB'}

        # Helper function to safely get node attributes
        def safe_get_attr(attr_name, default=None):
            return self.graph.nodes[node].get(attr_name, default)

        cstr_type = safe_get_attr('cstr_type')
        cstr_date = safe_get_attr('cstr_date')
        cstr_type2 = safe_get_attr('cstr_type2')
        cstr_date2 = safe_get_attr('cstr_date2')

        early_start = pd.Timestamp(self.early_start.get(node)) if self.early_start.get(node) else None
        late_finish = pd.Timestamp(self.late_finish.get(node)) if self.late_finish.get(node) else None

        def apply_constraint(constraint_type, constraint_date, early_start, late_finish):
            # Check for missing or invalid constraint type
            if not constraint_type or constraint_type not in VALID_CONSTRAINTS:
                self.logger.warning(f"Invalid or missing constraint type for task {node}: {constraint_type}")
                return early_start, late_finish

            # Check for missing constraint date
            if pd.isnull(constraint_date):
                self.logger.warning(f"Missing constraint date for task {node}, constraint type: {constraint_type}")
                return early_start, late_finish

            try:
                # Convert constraint_date to Timestamp
                constraint_date = pd.Timestamp(constraint_date)
                if pd.isnull(constraint_date):
                    return early_start, late_finish
                constraint_date = constraint_date.normalize()
            except (ValueError, TypeError):
                self.logger.error(f"Invalid constraint date for task {node}: {constraint_date}")
                return early_start, late_finish

            # Ensure early_start and late_finish are Timestamps
            try:
                if early_start is not None and not pd.isnull(early_start):
                    early_start = pd.Timestamp(early_start).normalize()
                if late_finish is not None and not pd.isnull(late_finish):
                    late_finish = pd.Timestamp(late_finish).normalize()
            except (ValueError, TypeError) as e:
                self.logger.error(f"Invalid early_start or late_finish for task {node}: {str(e)}")
                return early_start, late_finish

            # Apply constraints
            if constraint_type == 'CS_ALAP':
                pass  # No action needed for As Late as Possible
            elif constraint_type in ['CS_MEO', 'CS_MANDFIN']:
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
                if early_start is not None:
                    early_start = max(early_start, constraint_date)
            elif constraint_type == 'CS_MSOB':
                if early_start is not None:
                    early_start = min(early_start, constraint_date)

            return early_start, late_finish

        # Apply primary constraint only if cstr_type is valid
        if cstr_type:
            early_start, late_finish = apply_constraint(cstr_type, cstr_date, early_start, late_finish)

        # Apply secondary constraint only if cstr_type2 is valid
        if cstr_type2:
            early_start, late_finish = apply_constraint(cstr_type2, cstr_date2, early_start, late_finish)

        # Check for conflicts only if we have both early_start and late_finish
        if early_start is not None and late_finish is not None and not pd.isna(early_start) and not pd.isna(
                late_finish):
            if early_start > late_finish:
                self.logger.warning(f"Conflict detected for task {node}: Early start is later than late finish")

        # Check if constraint violates predecessor relationships
        if is_forward_pass:
            for pred in self.graph.predecessors(node):
                if early_start is not None and self.early_finish[pred] is not None:
                    if not pd.isna(early_start) and not pd.isna(self.early_finish[pred]):
                        if pd.Timestamp(early_start).normalize() < pd.Timestamp(self.early_finish[pred]).normalize():
                            self.logger.warning(f"Constraint for task {node} violates predecessor {pred} relationship")

        return early_start, late_finish

    def set_workday_df(self, workday):
        self.workdays_df = workday.copy()

    def set_exception_df(self, exception):
        self.exceptions_df = exception.copy()

    def build_graph(self):
        for _, task in self.xer.task_df.iterrows():
            task_type = task['task_type']

            # Handle different task types
            if task_type in ['TT_Mile', 'TT_FinMile']:
                duration = 0  # Milestones have zero duration
            elif task_type == 'TT_LOE':
                # Level of Effort tasks are handled separately
                duration = pd.to_timedelta(pd.to_numeric(task['target_drtn_hr_cnt']), unit='h').days
            elif task_type in ['TT_Task', 'TT_Rsrc']:
                # Regular tasks and resource-dependent tasks
                duration = pd.to_timedelta(pd.to_numeric(task['target_drtn_hr_cnt']), unit='h').days
            elif task_type == 'TT_WBS':
                # WBS summary tasks
                duration = 0  # Duration will be calculated based on subtasks
            else:
                print(f"Warning: Unknown task type {task_type} for task {task['task_id']}")
                duration = 0

            act_start_date = pd.to_datetime(task['act_start_date'], errors='coerce')
            act_end_date = pd.to_datetime(task['act_end_date'], errors='coerce')

            self.graph.add_node(task['task_id'],
                                duration=duration,
                                calendar_id=task['clndr_id'],
                                cstr_type=task['cstr_type'],
                                cstr_date=task['cstr_date'],
                                cstr_type2=task['cstr_type2'],
                                cstr_date2=task['cstr_date2'],
                                act_start_date=act_start_date,
                                act_end_date=act_end_date,
                                task_type=task_type,
                                is_loe=(task_type == 'TT_LOE'),
                                wbs_id=task['wbs_id'])  # Added WBS ID

        for _, pred in self.xer.taskpred_df.iterrows():
            try:
                lag = pd.to_timedelta(pd.to_numeric(pred['lag_hr_cnt']), unit='h')
            except ValueError:
                print(
                    f"Warning: Invalid lag for relationship {pred['pred_task_id']} -> {pred['task_id']}: {pred['lag_hr_cnt']}")
                lag = pd.Timedelta(0, unit='h')
            self.graph.add_edge(pred['pred_task_id'], pred['task_id'], lag=lag, taskpred_type=pred['pred_type'])

        # Handle WBS summary tasks
        for node in self.graph.nodes:
            if self.graph.nodes[node]['task_type'] == 'TT_WBS':
                self.graph.nodes[node]['subtasks'] = self.get_subtasks(node)

    def forward_pass(self):
        project_start = pd.to_datetime(self.xer.project_df['plan_start_date'].iloc[0])
        self.logger.info("Starting forward pass")

        # Initialize early start times for all nodes
        for node in self.graph.nodes:
            self.early_start[node] = None
            self.early_finish[node] = None

        for node in nx.topological_sort(self.graph):
            task_type = self.graph.nodes[node]['task_type']
            act_start = self.graph.nodes[node]['act_start_date']
            act_end = self.graph.nodes[node]['act_end_date']

            if task_type == 'TT_LOE':
                # Level of Effort tasks are scheduled based on their dependencies
                continue

            if pd.notnull(act_start) and act_start <= self.data_date:
                # Task has started
                self.early_start[node] = act_start
                if pd.notnull(act_end) and act_end <= self.data_date:
                    # Task is completed
                    self.early_finish[node] = act_end
                else:
                    # Task is in progress
                    calculated_finish = self.working_day_calculator.add_working_days(
                        act_start,
                        self.graph.nodes[node]['duration'],
                        self.graph.nodes[node]['calendar_id']
                    )
                    self.early_finish[node] = max(self.data_date, pd.Timestamp(calculated_finish))
            else:
                # Task hasn't started yet
                predecessors = list(self.graph.predecessors(node))
                if not predecessors:
                    self.early_start[node] = max(project_start, self.data_date)
                else:
                    pred_finish_dates = []
                    for pred in predecessors:
                        if self.early_finish[pred] is None:
                            self.logger.warning(f"Predecessor {pred} of {node} has no early finish date.")
                            continue
                        pred_finish = self.early_finish[pred]
                        taskpred_type = self.graph[pred][node]['taskpred_type']
                        lag = self.graph[pred][node]['lag']

                        if taskpred_type == 'PR_FS':  # Finish-to-Start
                            pred_finish_dates.append(pred_finish + lag)
                        elif taskpred_type == 'PR_SS':  # Start-to-Start
                            pred_finish_dates.append(self.early_start[pred] + lag)
                        elif taskpred_type == 'PR_FF':  # Finish-to-Finish
                            pred_finish_dates.append(
                                pred_finish + lag - pd.Timedelta(days=self.graph.nodes[node]['duration']))
                        elif taskpred_type == 'PR_SF':  # Start-to-Finish
                            pred_finish_dates.append(
                                self.early_start[pred] + lag - pd.Timedelta(days=self.graph.nodes[node]['duration']))

                    if pred_finish_dates:
                        self.early_start[node] = max(max(pred_finish_dates), self.data_date)
                    else:
                        self.early_start[node] = max(project_start, self.data_date)

                # Apply constraints
                self.early_start[node], _ = self.apply_activity_constraints(node, is_forward_pass=True)

                # Calculate early finish
                if task_type in ['TT_Mile', 'TT_FinMile']:
                    self.early_finish[node] = self.early_start[node]
                elif task_type == 'TT_WBS':
                    # For WBS summary tasks, early finish is the max of its subtasks
                    subtasks = self.graph.nodes[node]['subtasks']
                    if subtasks:
                        self.early_finish[node] = max(self.early_finish[subtask] for subtask in subtasks)
                    else:
                        self.early_finish[node] = self.early_start[node]
                else:
                    calculated_finish = self.working_day_calculator.add_working_days(
                        self.early_start[node],
                        self.graph.nodes[node]['duration'],
                        self.graph.nodes[node]['calendar_id']
                    )
                    self.early_finish[node] = pd.Timestamp(calculated_finish)

        # Handle LOE tasks after all other tasks have been scheduled
        for node in self.graph.nodes:
            if self.graph.nodes[node]['task_type'] == 'TT_LOE':
                predecessors = list(self.graph.predecessors(node))
                successors = list(self.graph.successors(node))

                if predecessors:
                    predecessor_starts = [self.early_start[pred] for pred in predecessors if
                                          self.early_start[pred] is not None]
                    self.early_start[node] = min(predecessor_starts) if predecessor_starts else self.data_date
                else:
                    self.early_start[node] = self.data_date

                if successors:
                    successor_finishes = [self.early_finish[succ] for succ in successors if
                                          self.early_finish[succ] is not None]
                    if successor_finishes:
                        self.early_finish[node] = max(successor_finishes)
                    else:
                        # If all successors have None as early_finish, use project end date
                        non_none_finishes = [finish for finish in self.early_finish.values() if finish is not None]
                        self.early_finish[node] = max(non_none_finishes) if non_none_finishes else self.data_date
                else:
                    # If there are no successors, use project end date
                    non_none_finishes = [finish for finish in self.early_finish.values() if finish is not None]
                    self.early_finish[node] = max(non_none_finishes) if non_none_finishes else self.data_date

        self.logger.info("Forward pass completed")

    def backward_pass(self):
        self.logger.info("Starting backward pass")

        # Find the latest early finish date as the project end date
        project_end = max(finish for finish in self.early_finish.values() if finish is not None)

        # Initialize late finish times for all nodes
        for node in self.graph.nodes:
            self.late_start[node] = None
            self.late_finish[node] = None

        for node in reversed(list(nx.topological_sort(self.graph))):
            task_type = self.graph.nodes[node]['task_type']
            act_start = self.graph.nodes[node]['act_start_date']
            act_end = self.graph.nodes[node]['act_end_date']

            if task_type == 'TT_LOE':
                # Level of Effort tasks are scheduled based on their dependencies
                continue

            if pd.notnull(act_end) and act_end <= self.data_date:
                # Task is completed
                self.late_finish[node] = act_end
                self.late_start[node] = act_start
            elif pd.notnull(act_start) and act_start <= self.data_date:
                # Task is in progress
                self.late_start[node] = act_start
                calculated_finish = self.working_day_calculator.add_working_days(
                    act_start,
                    self.graph.nodes[node]['duration'],
                    self.graph.nodes[node]['calendar_id']
                )
                self.late_finish[node] = max(pd.Timestamp(self.data_date), pd.Timestamp(calculated_finish))
            else:
                # Task hasn't started yet
                successors = list(self.graph.successors(node))
                if not successors:
                    self.late_finish[node] = project_end
                else:
                    succ_dates = []
                    for succ in successors:
                        if self.late_start[succ] is None:
                            self.logger.warning(f"Successor {succ} of {node} has no late start date.")
                            continue
                        succ_start = self.late_start[succ]
                        taskpred_type = self.graph[node][succ]['taskpred_type']
                        lag = self.graph[node][succ]['lag']

                        if taskpred_type == 'PR_FS':  # Finish-to-Start
                            succ_dates.append(succ_start - lag)
                        elif taskpred_type == 'PR_SS':  # Start-to-Start
                            succ_dates.append(succ_start - lag)
                        elif taskpred_type == 'PR_FF':  # Finish-to-Finish
                            succ_dates.append(self.late_finish[succ] - lag)
                        elif taskpred_type == 'PR_SF':  # Start-to-Finish
                            succ_dates.append(self.late_finish[succ] - lag)

                    if succ_dates:
                        self.late_finish[node] = min(succ_dates)
                    else:
                        self.late_finish[node] = project_end

                # Apply constraints
                _, self.late_finish[node] = self.apply_activity_constraints(node, is_forward_pass=False)

                # Calculate late start
                if task_type in ['TT_Mile', 'TT_FinMile']:
                    self.late_start[node] = self.late_finish[node]
                elif task_type == 'TT_WBS':
                    # For WBS summary tasks, late start is the min of its subtasks
                    subtasks = self.graph.nodes[node]['subtasks']
                    if subtasks:
                        self.late_start[node] = min(self.late_start[subtask] for subtask in subtasks)
                    else:
                        self.late_start[node] = self.late_finish[node]
                else:
                    calculated_start = self.working_day_calculator.add_working_days(
                        self.late_finish[node],
                        -self.graph.nodes[node]['duration'],
                        self.graph.nodes[node]['calendar_id']
                    )
                    self.late_start[node] = pd.Timestamp(calculated_start)

            # Ensure late dates are not earlier than data date for future tasks
            if pd.isnull(act_start) or act_start > self.data_date:
                self.late_start[node] = max(pd.Timestamp(self.late_start[node]), pd.Timestamp(self.data_date))
                self.late_finish[node] = max(pd.Timestamp(self.late_finish[node]), pd.Timestamp(self.data_date))

        # Handle LOE tasks after all other tasks have been scheduled
        for node in self.graph.nodes:
            if self.graph.nodes[node]['task_type'] == 'TT_LOE':
                predecessors = list(self.graph.predecessors(node))
                successors = list(self.graph.successors(node))

                if successors:
                    successor_starts = [self.late_start[succ] for succ in successors if
                                        self.late_start[succ] is not None]
                    if successor_starts:
                        self.late_start[node] = min(successor_starts)
                    else:
                        self.late_start[node] = project_end
                else:
                    self.late_start[node] = project_end

                if predecessors:
                    predecessor_finishes = [self.late_finish[pred] for pred in predecessors if
                                            self.late_finish[pred] is not None]
                    if predecessor_finishes:
                        self.late_finish[node] = max(predecessor_finishes)
                    else:
                        self.late_finish[node] = project_end
                else:
                    self.late_finish[node] = project_end

        self.logger.info("Backward pass completed")

    def calculate_total_float(self):
        for node in self.graph.nodes:
            task_type = self.graph.nodes[node]['task_type']

            if task_type == 'TT_LOE':
                # Level of Effort tasks are not on the critical path
                self.total_float[node] = float('inf')
            elif task_type == 'TT_WBS':
                # WBS Summary tasks' float is the minimum float of their subtasks
                subtasks = self.graph.nodes[node]['subtasks']
                if subtasks:
                    self.total_float[node] = min(
                        self.total_float[subtask] for subtask in subtasks if subtask in self.total_float)
                else:
                    self.total_float[node] = 0
            elif pd.notnull(self.graph.nodes[node]['act_end_date']) and self.graph.nodes[node][
                'act_end_date'] <= self.data_date:
                # Completed tasks have zero float
                self.total_float[node] = 0
            else:
                # Calculate float for other task types
                early_start = max(self.early_start[node], self.data_date)
                late_start = self.late_start[node]

                if pd.isnull(early_start) or pd.isnull(late_start):
                    self.logger.warning(
                        f"Unable to calculate total float for task {node}: missing early or late start.")
                    self.total_float[node] = None
                else:
                    self.total_float[node] = self.working_day_calculator.get_working_days_between(
                        early_start,
                        late_start,
                        self.graph.nodes[node]['calendar_id']
                    )

            # Log any negative float as it might indicate a scheduling issue
            if self.total_float[node] is not None and self.total_float[node] < 0:
                self.logger.warning(f"Negative total float detected for task {node}: {self.total_float[node]}")

    def determine_critical_path(self, float_threshold=0):
        self.critical_path = []
        critical_tasks = []
        completed_critical_tasks = []

        for node, data in self.graph.nodes(data=True):
            task_type = data['task_type']
            total_float = self.total_float.get(node)
            is_completed = pd.notnull(data['act_end_date']) and data['act_end_date'] <= self.data_date

            if task_type == 'TT_LOE':
                continue  # Exclude Level of Effort tasks from critical path

            if total_float is None:
                self.logger.warning(
                    f"Task {node} has no total float calculated. Skipping in critical path determination.")
                continue

            if total_float <= float_threshold:
                if is_completed:
                    completed_critical_tasks.append(node)
                else:
                    critical_tasks.append(node)

        # Sort non-completed critical tasks topologically
        try:
            critical_subgraph = self.graph.subgraph(critical_tasks)
            sorted_critical_tasks = list(nx.topological_sort(critical_subgraph))
        except nx.NetworkXUnfeasible:
            self.logger.error("Critical path contains a cycle. Unable to determine a valid critical path.")
            return

        # Combine completed and non-completed critical tasks
        self.critical_path = completed_critical_tasks + sorted_critical_tasks

        # Log completed critical tasks
        if completed_critical_tasks:
            self.logger.info(f"Completed critical tasks: {', '.join(map(str, completed_critical_tasks))}")

        # Verify critical path continuity
        for i in range(len(sorted_critical_tasks) - 1):
            current_task = sorted_critical_tasks[i]
            next_task = sorted_critical_tasks[i + 1]
            if next_task not in self.graph.successors(current_task):
                self.logger.warning(f"Discontinuity in critical path between tasks {current_task} and {next_task}")

        # Check if critical path starts from a start task and ends at an end task
        start_tasks = [node for node in self.graph.nodes if not list(self.graph.predecessors(node))]
        end_tasks = [node for node in self.graph.nodes if not list(self.graph.successors(node))]

        if self.critical_path and self.critical_path[0] not in start_tasks:
            self.logger.warning("Critical path does not start from a project start task.")

        if self.critical_path and self.critical_path[-1] not in end_tasks:
            self.logger.warning("Critical path does not end at a project end task.")

        # Log the identified critical path
        self.logger.info(f"Critical path identified: {' -> '.join(map(str, self.critical_path))}")

        # Calculate and log the critical path length
        if self.critical_path:
            critical_path_length = sum(self.graph.nodes[node]['duration'] for node in self.critical_path
                                       if not pd.notnull(self.graph.nodes[node]['act_end_date']))
            self.logger.info(f"Critical path length: {critical_path_length} days")

        # Identify near-critical paths
        near_critical_threshold = float_threshold * 1.1  # 10% more than the critical threshold
        near_critical_tasks = [node for node, data in self.graph.nodes(data=True)
                               if self.total_float.get(node, float('inf')) <= near_critical_threshold
                               and node not in self.critical_path]

        if near_critical_tasks:
            self.logger.info(f"Near-critical tasks identified: {', '.join(map(str, near_critical_tasks))}")

        return self.critical_path

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
        self.xer.task_df['target_start_date'] = self.xer.task_df.apply(self.calculate_forecast_start, axis=1)
        self.xer.task_df['target_end_date'] = self.xer.task_df.apply(self.calculate_forecast_finish, axis=1)

    def calculate_forecast_start(self, task):
        if pd.notnull(task['act_end_date']) and task['act_end_date'] <= self.data_date:
            return task['act_start_date']
        else:
            return self.early_start[task['task_id']]

    def calculate_forecast_finish(self, task):
        if pd.notnull(task['act_end_date']) and task['act_end_date'] <= self.data_date:
            return task['act_end_date']
        else:
            forecast_start = self.calculate_forecast_start(task)
            duration = self.graph.nodes[task['task_id']]['duration']
            calendar_id = self.graph.nodes[task['task_id']]['calendar_id']
            forecast_finish = self.working_day_calculator.add_working_days(forecast_start, duration, calendar_id)
            return forecast_finish

    def get_project_duration(self):
        if not self.critical_path:
            return None
        plan_start_date = pd.to_datetime(self.xer.project_df['plan_start_date'].iloc[0])
        return self.late_finish[self.critical_path[-1]] - plan_start_date

    def print_results(self):
        print("Task ID\tES\t\tEF\t\tLS\t\tLF\t\tTotal Float\tCritical")
        for _, task in self.xer.task_df.iterrows():
            print(f"{task['task_id']}\t{task['task_code']}\t{task['task_name']}\t"
                  f"{task['target_start_date']}\t{task['target_end_date']}\t{task['total_float']}\t{task['is_critical']}")
        project_duration = self.get_project_duration()
        if project_duration == pd.Timedelta(0):
            print("\nCritical Path: Not found")
            print("Project Duration: Unable to determine")
        else:
            print(f"\nCritical Path: {' -> '.join(map(str, self.critical_path))}")
            print(f"Project Duration: {project_duration.days} days")

    def generate_critical_path_report(self):
        mdFile = MdUtils(file_name='Critical_Path_Report', title='Critical Path Report')

        mdFile.new_header(level=1, title='Project Information')
        project_info = self.xer.project_df.iloc[0]
        mdFile.new_paragraph(f"Project Name: {project_info['proj_short_name']}")
        mdFile.new_paragraph(f"Project ID: {project_info['proj_id']}")
        mdFile.new_paragraph(f"Data Date: {self.data_date.strftime('%Y-%m-%d')}")

        # Milestone Table
        mdFile.new_header(level=1, title='Project Milestones')
        milestone_table = ["Task ID", "Task Name", "Date", "Type"]

        milestone_tasks = self.xer.task_df[self.xer.task_df['task_type'].isin(['TT_Mile', 'TT_FinMile'])]

        for _, task in milestone_tasks.iterrows():
            task_id = task['task_id']
            task_name = task['task_name']

            if pd.notnull(task['act_start_date']):
                date = task['act_start_date'].strftime('%Y-%m-%d')
                date_type = "Actual Start"
            elif pd.notnull(task['act_end_date']):
                date = task['act_end_date'].strftime('%Y-%m-%d')
                date_type = "Actual End"
            elif pd.notnull(task['target_start_date']):
                date = task['target_start_date'].strftime('%Y-%m-%d')
                date_type = "Target Start"
            elif pd.notnull(task['target_end_date']):
                date = task['target_end_date'].strftime('%Y-%m-%d')
                date_type = "Target End"
            else:
                date = "N/A"
                date_type = "N/A"

            milestone_table.extend([task_id, task_name, date, date_type])

        mdFile.new_table(columns=4, rows=len(milestone_tasks) + 1, text=milestone_table, text_align='left')

        # Critical Path Table
        mdFile.new_header(level=1, title='Critical Path')

        cp_table = ["Task ID", "Task Name", "Start Date", "End Date", "Total Float"]

        for task_id in self.critical_path:
            task = self.xer.task_df[self.xer.task_df['task_id'] == task_id].iloc[0]
            task_name = task['task_name']

            if task['task_type'] in ['TT_Mile', 'TT_FinMile']:
                if pd.notnull(task['act_start_date']):
                    start_date = end_date = task['act_start_date'].strftime('%Y-%m-%d')
                elif pd.notnull(task['act_end_date']):
                    start_date = end_date = task['act_end_date'].strftime('%Y-%m-%d')
                elif pd.notnull(task['target_start_date']):
                    start_date = end_date = task['target_start_date'].strftime('%Y-%m-%d')
                elif pd.notnull(task['target_end_date']):
                    start_date = end_date = task['target_end_date'].strftime('%Y-%m-%d')
                else:
                    start_date = end_date = "N/A"
            else:
                start_date = self.early_start[task_id].strftime('%Y-%m-%d')
                end_date = self.early_finish[task_id].strftime('%Y-%m-%d')

            total_float = f"{self.total_float[task_id]:.2f}"

            cp_table.extend([task_id, task_name, start_date, end_date, total_float])

        mdFile.new_table(columns=5, rows=len(self.critical_path) + 1, text=cp_table, text_align='left')

        mdFile.create_md_file()
        self.logger.info("Critical Path Report generated successfully.")

    def get_subtasks(self, wbs_node):
        """
        Get the subtasks of a WBS summary task.

        Args:
        wbs_node (str): The task_id of the WBS summary task.

        Returns:
        list: A list of task_ids that are subtasks of the given WBS summary task.
        """
        subtasks = []

        # Get the WBS information for the summary task
        wbs_info = self.xer.projwbs_df[self.xer.projwbs_df['wbs_id'] == self.graph.nodes[wbs_node]['wbs_id']]

        if wbs_info.empty:
            return subtasks

        wbs_path = wbs_info['wbs_short_name'].iloc[0]

        # Find all tasks that belong to this WBS or its sub-WBS
        for task_id, task_data in self.graph.nodes(data=True):
            task_wbs_id = task_data.get('wbs_id')
            if task_wbs_id:
                task_wbs_info = self.xer.projwbs_df[self.xer.projwbs_df['wbs_id'] == task_wbs_id]
                if not task_wbs_info.empty:
                    task_wbs_path = task_wbs_info['wbs_short_name'].iloc[0]
                    if task_wbs_path.startswith(wbs_path) and task_id != wbs_node:
                        subtasks.append(task_id)

        return subtasks
