import os
import socket
import random
import pandas as pd
from datetime import datetime, timedelta, date
from collections import defaultdict
import math
import copy

#####################
# PATHS
#####################
def get_base_path():
    if os.name == 'nt':
        return r'C:\Users\josemaria\Downloads'
    else:
        hostname = socket.gethostname()
        if hostname == 'MacBook-Pro.local':
            return '/Users/j.m./Users/jm/Library/Mobile Documents/com~apple~CloudDocs/GM/MOBU - OPL/Planilla/'
        elif hostname == 'JM-MS.local':
            return '/Users/jm/Library/Mobile Documents/com~apple~CloudDocs/GM/MOBU - OPL/Planilla/'
        else:
            print(f"Warning: Unknown hostname {hostname}. Returning None")
            return None

def get_base_output_path():
    if os.name == 'nt':
        return r'C:\Users\josemaria\Downloads'
    else:
        hostname = socket.gethostname()
        if hostname == 'MacBook-Pro.local':
            return '/Users/j.m./Downloads'
        elif hostname == 'JM-MS.local':
            return '/Users/jm/Downloads'
        else:
            print(f"Warning: Unknown hostname {hostname} for output path.")
            return None

#####################
# PEAK / FESTIVE / SUNDAY
#####################
def is_peak_season(date_obj):
    """True if date_obj is in [Oct 1 - Jan 16], if skip_peak=True we skip these days."""
    m, d = date_obj.month, date_obj.day
    if m in [10, 11, 12]:
        return True
    if m == 1 and d <= 16:
        return True
    return False

def is_festive_day(date_obj, festive_days):
    return date_obj in festive_days

def is_sunday(date_obj):
    return (date_obj.weekday() == 6)

def add_days_continuous(start_date, length_days):
    return start_date + timedelta(days=length_days - 1)

#####################
# TASK BUILD
#####################
def build_tasks(df, analysis_year):
    """
    If employee's Fecha Ingreso < Jan 1(analysis_year) => eligible.
    quincenal => 15 days
    semanal => block#1 => 7 days, block#2 => 8 days
    """
    tasks = []
    cutoff_date = date(analysis_year, 1, 1)

    for idx, row in df.iterrows():
        cargo = row['Cargo']
        emp_id = row['Codigo Empleado']
        name = row['Nombre Completo']
        mod = row['modalidad']
        fecha_ingreso = pd.to_datetime(row['Fecha Ingreso']).date()

        # eligibility
        if fecha_ingreso >= cutoff_date:
            continue

        if mod == 'quincenal':
            tasks.append({
                'employee_id': emp_id,
                'employee_name': name,
                'cargo': cargo,
                'block_number': 1,  # only one block
                'vac_length': 15,
                'status': 'waiting',
                'start_date': None,
                'end_date': None
            })
        elif mod == 'semanal':
            # block1 => 7
            tasks.append({
                'employee_id': emp_id,
                'employee_name': name,
                'cargo': cargo,
                'block_number': 1,
                'vac_length': 7,
                'status': 'waiting',
                'start_date': None,
                'end_date': None
            })
            # block2 => 8
            tasks.append({
                'employee_id': emp_id,
                'employee_name': name,
                'cargo': cargo,
                'block_number': 2,
                'vac_length': 8,
                'status': 'waiting',
                'start_date': None,
                'end_date': None
            })
    return tasks

#####################
# TRY WEEKLY BATCH with global concurrency
#####################
def try_schedule_weekly_batch(tasks, start_day, last_day, festive_days,
                              global_concurrency,
                              max_starts_per_week,
                              skip_peak=True):
    """
    We'll interpret `global_concurrency` as the concurrency limit for *all* cargos.
    Then do the weekly-batch approach with max_starts_per_week.
    Return (scheduled_tasks, success_bool).
    """
    # deep copy
    tasks = copy.deepcopy(tasks)
    # shuffle
    # random.shuffle(tasks)
    n_tasks = len(tasks)
    assigned_count = 0

    # build weeks
    week_starts = []
    cur_week = start_day
    while cur_week <= last_day:
        week_starts.append(cur_week)
        cur_week += timedelta(days=7)

    cargo_usage = defaultdict(int)  # usage by cargo
    running_tasks = []

    def free_finished(current_date):
        finished = []
        for rt in running_tasks:
            if current_date > rt['end_date']:
                rt['status'] = 'done'
                finished.append(rt)
        for f in finished:
            running_tasks.remove(f)
            c = f['cargo'].lower().strip()
            cargo_usage[c] -= 1

    idx_task = 0

    for ws in week_starts:
        weekly_quota = max_starts_per_week
        for dd in range(7):
            day_date = ws + timedelta(days=dd)
            if day_date > last_day:
                break
            free_finished(day_date)

            dt_today = datetime.combine(day_date, datetime.min.time())
            if is_sunday(dt_today):
                continue
            if skip_peak and is_peak_season(dt_today):
                continue
            if is_festive_day(dt_today, festive_days):
                continue

            while idx_task < n_tasks and weekly_quota > 0:
                t = tasks[idx_task]
                if t['status'] != 'waiting':
                    idx_task += 1
                    continue

                c_lower = t['cargo'].lower().strip()
                usage = cargo_usage[c_lower]
                if usage >= global_concurrency:
                    idx_task += 1
                    continue

                # start
                t['status'] = 'running'
                t['start_date'] = day_date
                t['end_date'] = add_days_continuous(day_date, t['vac_length'])
                running_tasks.append(t)
                cargo_usage[c_lower] += 1

                assigned_count += 1
                weekly_quota -= 1
                idx_task += 1

                if assigned_count == n_tasks:
                    free_finished(last_day + timedelta(days=1))
                    return (tasks, True)
        # end day for loop
    # end week

    free_finished(last_day + timedelta(days=1))
    if assigned_count == n_tasks:
        return (tasks, True)
    return (tasks, False)

#####################
# BLOCK1 + BLOCK2 with concurrency
#####################
def schedule_block12_global_conc(block1_tasks, block2_tasks,
                                 start_day, last_day, festive_days,
                                 global_concurrency,
                                 max_starts_per_week,
                                 skip_peak=True):
    """
    1) schedule block1
    2) if success => day after last block1 end => block2
    3) return (final_tasks, success)
    """
    b1_sched, ok_b1 = try_schedule_weekly_batch(block1_tasks, start_day, last_day,
                                                festive_days,
                                                global_concurrency,
                                                max_starts_per_week,
                                                skip_peak)
    if not ok_b1:
        return ([], False)

    b1_end_dates = [t['end_date'] for t in b1_sched if t['end_date']]
    if b1_end_dates:
        block2_start = max(b1_end_dates) + timedelta(days=1)
    else:
        block2_start = start_day

    b2_sched, ok_b2 = try_schedule_weekly_batch(block2_tasks,
                                                block2_start, last_day,
                                                festive_days,
                                                global_concurrency,
                                                max_starts_per_week,
                                                skip_peak)
    if not ok_b2:
        return ([], False)

    return (b1_sched + b2_sched, True)

#####################
# MAIN SCHEDULING
#####################
def generate_vacation_schedule(df, festive_days, analysis_year=2025):
    """
    1) build tasks
    2) separate block1 vs block2
    3) define search for concurrency=1..some_upper
       for each concurrency => for max_starts_per_week=2..some_upper
         for attempt in range(50):
            random scheduling => if success => done
    """
    df = df[df['Tipo'].isin(['Jubilados','Permanentes'])]
    df['Fecha Ingreso'] = pd.to_datetime(df['Fecha Ingreso'])

    start_day = date(analysis_year, 1, 17)
    last_day = date(analysis_year, 9, 30)

    all_tasks = build_tasks(df, analysis_year)
    if not all_tasks:
        return pd.DataFrame()

    # separate block1 vs block2
    block1 = []
    block2 = []
    for t in all_tasks:
        if t['block_number'] == 1:
            block1.append(t)
        else:
            block2.append(t)

    total_tasks = len(all_tasks)

    # We'll define an upper bound for concurrency (like up to total_tasks or 10)
    max_concurrency = 5
    attempts_per_combo = 2000

    for concurrency_lvl in range(1, max_concurrency+1):
        for per_week in range(2, total_tasks+1):
            # try up to 50 times for the same concurrency/per_week
            for attempt in range(attempts_per_combo):
                final_sched, success = schedule_block12_global_conc(
                    block1, block2,
                    start_day, last_day,
                    festive_days,
                    global_concurrency=concurrency_lvl,
                    max_starts_per_week=per_week,
                    skip_peak=True
                )
                if success:
                    print(f"Success with concurrency={concurrency_lvl}, "
                          f"max_starts_per_week={per_week} "
                          f"on attempt={attempt+1}")
                    # build df
                    rows = []
                    for x in final_sched:
                        rows.append({
                            'Nombre Completo': x['employee_name'],
                            'Modalidad': ('quincenal' if x['vac_length'] == 15 else 'semanal'),
                            'Cargo': x['cargo'],
                            'Block #': x['block_number'],
                            'Vacation Start': x['start_date'],
                            'Vacation End': x['end_date']
                        })
                    schedule_df = pd.DataFrame(rows)
                    schedule_df.sort_values(by=['Vacation Start','Nombre Completo','Block #'],
                                            inplace=True)
                    schedule_df.reset_index(drop=True, inplace=True)
                    print("Proposed Vacation Schedule:\n", schedule_df)
                    return schedule_df

    # if we never succeed
    print("Unable to schedule even with concurrency up to 10 and multiple attempts.")
    return pd.DataFrame()

#####################
# MAIN
#####################
if __name__ == '__main__':
    analysis_year = 2025

    base_path = get_base_path()
    if base_path is None:
        raise ValueError("No base path found")

    file_path = os.path.join(base_path, "MORIBUS Personal bodegas.xlsx")
    df = pd.read_excel(file_path, sheet_name="Empleados")
    df['Fecha Ingreso'] = pd.to_datetime(df['Fecha Ingreso'])
    df['month'] = df['Fecha Ingreso'].dt.month
    df = df.sort_values(by='month', ascending=True)

    pd.set_option("display.max_rows", 200)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 1000)
    pd.set_option("display.expand_frame_repr", False)

    print("Payroll:\n", df)

    # Example: 1 festive day
    festive_days = [
        datetime(analysis_year, 5, 10),
    ]

    schedule_df = generate_vacation_schedule(df, festive_days, analysis_year)

    out_path = get_base_output_path()
    if out_path:
        out_file = os.path.join(out_path, f"distribucion_vacaciones_MOBU_{analysis_year}.xlsx")
        schedule_df.to_excel(out_file, index=False)
    else:
        print("No output path found. Skipping save.")