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
            act_start = self.graph.nodes[node]['act_start_date']
            act_end = self.graph.nodes[node]['act_end_date']

            if pd.notnull(act_start) and act_start <= self.data_date:
                # Task has started
                self.early_start[node] = act_start
                if pd.notnull(act_end) and act_end <= self.data_date:
                    # Task is completed
                    self.early_finish[node] = act_end
                else:
                    # Task is in progress
                    # Convert the result of add_working_days to Timestamp
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
                calculated_finish = self.working_day_calculator.add_working_days(
                    self.early_start[node],
                    self.graph.nodes[node]['duration'],
                    self.graph.nodes[node]['calendar_id']
                )
                self.early_finish[node] = pd.Timestamp(calculated_finish)

        self.logger.info("Forward pass completed")

    def backward_pass(self):
        self.logger.info("Starting backward pass")

        # Find the latest early finish date as the project end date
        project_end = max(self.early_finish.values())

        # Initialize late finish times for all nodes
        for node in self.graph.nodes:
            self.late_start[node] = None
            self.late_finish[node] = None

        for node in reversed(list(nx.topological_sort(self.graph))):
            act_start = self.graph.nodes[node]['act_start_date']
            act_end = self.graph.nodes[node]['act_end_date']

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
                calculated_start = self.working_day_calculator.add_working_days(
                    self.late_finish[node],
                    -self.graph.nodes[node]['duration'],
                    self.graph.nodes[node]['calendar_id']
                )
                self.late_start[node] = pd.Timestamp(calculated_start)  # Ensure late_start is a Timestamp

            # Ensure late dates are not earlier than data date for future tasks
            if pd.isnull(act_start) or act_start > self.data_date:
                self.late_start[node] = max(pd.Timestamp(self.late_start[node]), pd.Timestamp(self.data_date))
                self.late_finish[node] = max(pd.Timestamp(self.late_finish[node]), pd.Timestamp(self.data_date))

        self.logger.info("Backward pass completed")
    def calculate_total_float(self):
        for node in self.graph.nodes:
            if pd.notnull(self.graph.nodes[node]['act_end_date']) and self.graph.nodes[node][
                'act_end_date'] <= self.data_date:
                self.total_float[node] = 0
            else:
                self.total_float[node] = self.working_day_calculator.get_working_days_between(
                    max(self.early_start[node], self.data_date),
                    self.late_start[node],
                    self.graph.nodes[node]['calendar_id']
                )

    def determine_critical_path(self, float_threshold=0):
        self.critical_path = []
        completed_tasks = set(node for node in self.graph.nodes if
                              pd.notnull(self.graph.nodes[node]['act_end_date']) and self.graph.nodes[node][
                                  'act_end_date'] <= self.data_date)

        # Identify tasks with total float less than or equal to the threshold
        critical_tasks = [node for node, tf in self.total_float.items() if tf <= float_threshold]

        # Include completed tasks with zero total float in the critical path
        critical_tasks.extend(node for node in completed_tasks if self.total_float[node] == 0)

        # Ensure continuity of the critical path
        for task in critical_tasks:
            if not self.critical_path:
                self.critical_path.append(task)
            else:
                predecessors = list(self.graph.predecessors(task))
                if any(pred in self.critical_path for pred in predecessors):
                    self.critical_path.append(task)

        # Verify that the critical path starts from the project start and ends at the project end
        start_tasks = [node for node in self.graph.nodes if not list(self.graph.predecessors(node))]
        end_tasks = [node for node in self.graph.nodes if not list(self.graph.successors(node))]

        if not any(start_task in self.critical_path for start_task in start_tasks):
            self.logger.warning("Critical path does not start from the project start task.")

        if not any(end_task in self.critical_path for end_task in end_tasks):
            self.logger.warning("Critical path does not end at the project end task.")
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
            print(f"{task['task_id']}\t{task['early_start']}\t{task['early_finish']}\t"
                  f"{task['late_start']}\t{task['late_finish']}\t{task['total_float']}\t{task['is_critical']}")
        project_duration = self.get_project_duration()
        if project_duration == pd.Timedelta(0):
            print("\nCritical Path: Not found")
            print("Project Duration: Unable to determine")
        else:
            print(f"\nCritical Path: {' -> '.join(map(str, self.critical_path))}")
            print(f"Project Duration: {project_duration.days} days")

    def generate_critical_path_report(self, file_path):
        mdFile = MdUtils(file_name=file_path, title='Critical Path Report')

        # Add project information
        mdFile.new_header(level=1, title='Project Information')
        for _, project in self.xer.project_df.iterrows():
            mdFile.new_paragraph(f"Project ID: {project['proj_id']}")
            mdFile.new_paragraph(f"Project Name: {project['proj_short_name']}")

        # Add critical path information
        mdFile.new_header(level=1, title='Critical Path')
        mdFile.new_line()

        # Create a table with the critical path tasks
        headers = ['Task Code', 'Task Name', 'Target Start Date', 'Target End Date']
        mdFile.new_table(columns=len(headers), rows=len(self.critical_path) + 1, text=headers, text_align='left')

        for task_id in self.critical_path:
            task = self.xer.task_df.loc[self.xer.task_df['task_id'] == task_id].iloc[0]
            task_code = task['task_code']
            task_name = task['task_name']
            target_start_date = task['target_start_date'].strftime('%Y-%m-%d')
            target_end_date = task['target_end_date'].strftime('%Y-%m-%d')
            mdFile.new_table_row([task_code, task_name, target_start_date, target_end_date])

        mdFile.create_md_file()

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