import os
import socket
import pandas as pd
from datetime import datetime, timedelta
from collections import defaultdict

#####################
# PATH FUNCTIONS
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
# LOGIC
#####################
def is_peak_season(date_obj):
    """True if date_obj is in [Oct 1 to Jan 16]."""
    m, d = date_obj.month, date_obj.day
    if m in [10, 11, 12]:
        return True
    if m == 1 and d <= 16:
        return True
    return False

def is_festive_day(date_obj, festive_days):
    """Check if date_obj is in festive_days."""
    return date_obj in festive_days

def is_sunday(date_obj):
    """Check if the date_obj is a Sunday (weekday=6)."""
    return date_obj.weekday() == 6

def add_days_continuous(start_date, length_days):
    """Vacation is continuous once it starts (including weekends/festives)."""
    return (start_date + timedelta(days=length_days - 1)).date()

def generate_tasks(df, last_day):
    """
    Build tasks:
      - quincenal => 1 block (15 days)
      - semanal   => 2 blocks (7 + 8 days = 15 total)
    Also has 'eligibility_date' = Fecha Ingreso + 365 days.
    We skip employees whose eligibility_date > last_day.
    """
    tasks = []
    for idx, row in df.iterrows():
        cargo = row['Cargo']
        emp_id = row['Codigo Empleado']
        name = row['Nombre Completo']
        mod = row['modalidad']
        fecha_ingreso = row['Fecha Ingreso']

        eligibility_date = (fecha_ingreso + timedelta(days=365)).date()
        if eligibility_date > last_day:
            # They won't be eligible by the end of our window
            continue

        if mod == 'quincenal':
            tasks.append({
                'employee_id': emp_id,
                'employee_name': name,
                'cargo': cargo,
                'block_number': 1,
                'vac_length': 15,
                'status': 'waiting',
                'start_date': None,
                'end_date': None,
                'eligibility_date': eligibility_date
            })
        elif mod == 'semanal':
            # 2 blocks => 7 days + 8 days
            tasks.append({
                'employee_id': emp_id,
                'employee_name': name,
                'cargo': cargo,
                'block_number': 1,
                'vac_length': 7,
                'status': 'waiting',
                'start_date': None,
                'end_date': None,
                'eligibility_date': eligibility_date
            })
            tasks.append({
                'employee_id': emp_id,
                'employee_name': name,
                'cargo': cargo,
                'block_number': 2,
                'vac_length': 8,
                'status': 'waiting',
                'start_date': None,
                'end_date': None,
                'eligibility_date': eligibility_date
            })
    return tasks

def generate_vacation_schedule(df, festive_days, analysis_year=2025):
    """
    Schedules from Jan 17 to Sep 30 of 'analysis_year', skipping:
      - Sundays, peak season, festive days for start
      - We allow concurrency across different cargos,
        but limit concurrency PER cargo to:
          * 2 for 'picker' and 'montacarguista'
          * 1 for all other cargos
      - 'semanal' => 2 blocks (7 & 8 days),
        block #2 waits until ALL first blocks for semanal are done
    """
    # Filter employees by type
    df['Fecha Ingreso'] = pd.to_datetime(df['Fecha Ingreso'])
    df = df[df['Tipo'].isin(['Jubilados','Permanentes'])]
    if df.empty:
        return pd.DataFrame()

    start_day = datetime(analysis_year, 1, 17).date()
    last_day = datetime(analysis_year, 9, 30).date()

    # Build tasks
    tasks = generate_tasks(df, last_day=last_day)
    if not tasks:
        return pd.DataFrame()

    # Identify first-block tasks for semanal employees
    semanal_ids = set(df.loc[df['modalidad'] == 'semanal','Codigo Empleado'])
    first_block_semanal = [
        t for t in tasks
        if t['employee_id'] in semanal_ids and t['block_number'] == 1
    ]
    num_semanal_first = len(first_block_semanal)

    def all_semanal_first_done():
        if num_semanal_first == 0:
            return True
        done_count = sum(t['status'] == 'done' for t in first_block_semanal)
        return (done_count == num_semanal_first)

    # Sort tasks
    tasks.sort(key=lambda x: (x['block_number'], x['cargo'], x['employee_id']))

    # cargo_concurrency_limit dict
    from collections import defaultdict
    cargo_concurrency_limit = defaultdict(lambda: 1)  # default = 1
    # Only 'picker' and 'montacarguista' get concurrency=2
    cargo_concurrency_limit['picker'] = 4
    cargo_concurrency_limit['montacarguista'] = 2
    cargo_concurrency_limit['coordinador de bodega'] = 2

    # track how many employees are running per cargo
    cargo_in_use_count = defaultdict(int)

    running_tasks = []

    def all_done():
        return all(t['status'] == 'done' for t in tasks)

    current_day = start_day
    while current_day <= last_day and not all_done():
        # 1) finish tasks that ended "yesterday"
        just_finished = []
        for rt in running_tasks:
            if current_day > rt['end_date']:
                rt['status'] = 'done'
                just_finished.append(rt)
        for jf in just_finished:
            running_tasks.remove(jf)
            cargo_name = jf['cargo'].lower().strip()
            cargo_in_use_count[cargo_name] -= 1

        # 2) can we start new tasks?
        dt_today = datetime.combine(current_day, datetime.min.time())
        can_start_today = True
        if is_sunday(dt_today) or is_peak_season(dt_today) or is_festive_day(dt_today, festive_days):
            can_start_today = False

        if can_start_today:
            # attempt to start waiting tasks
            for t in tasks:
                if t['status'] == 'waiting':
                    # if block #2 => wait for all block #1 done
                    if t['block_number'] == 2 and (t['vac_length'] in [7,8]):
                        if not all_semanal_first_done():
                            continue

                    # eligibility date
                    if current_day < t['eligibility_date']:
                        continue

                    # concurrency check
                    cargo_name = t['cargo'].lower().strip()
                    current_usage = cargo_in_use_count[cargo_name]
                    limit = cargo_concurrency_limit[cargo_name]

                    if current_usage >= limit:
                        continue  # cargo at capacity

                    # start this task
                    t['status'] = 'running'
                    t['start_date'] = current_day
                    t['end_date'] = add_days_continuous(dt_today, t['vac_length'])
                    running_tasks.append(t)
                    cargo_in_use_count[cargo_name] += 1

        # 3) next day
        current_day += timedelta(days=1)

    # build final DataFrame
    rows = []
    for t in tasks:
        rows.append({
            'Nombre Completo': t['employee_name'],
            'Modalidad': ('quincenal' if t['vac_length'] == 15 else 'semanal'),
            'Cargo': t['cargo'],
            'Block #': t['block_number'],
            'Vacation Start': t['start_date'],
            'Vacation End': t['end_date'],
            'Eligible On': t['eligibility_date']
        })
    schedule_df = pd.DataFrame(rows)
    schedule_df.sort_values(by=['Vacation Start','Nombre Completo','Block #'], inplace=True)
    schedule_df.reset_index(drop=True, inplace=True)
    return schedule_df

#####################
# MAIN
#####################
if __name__ == '__main__':
    analysis_year = 2025

    base_path = get_base_path()
    if base_path is None:
        raise ValueError("Could not determine base path. Check hostname or OS.")

    file_path = os.path.join(base_path, "MORIBUS Personal bodegas.xlsx")
    df = pd.read_excel(file_path, sheet_name="Empleados")

    # Example festive days
    festive_days = [
        datetime(analysis_year, 5, 10),
    ]

    schedule_df = generate_vacation_schedule(df, festive_days, analysis_year=analysis_year)

    # Adjust printing so lines don't break
    pd.set_option("display.max_rows", 200)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 1000)
    pd.set_option("display.expand_frame_repr", False)

    print(schedule_df)

    # If you want to save:
    out_path = get_base_output_path()
    if out_path:
        out_file = os.path.join(out_path, f"distribucion_vacaciones_MOBU_{analysis_year}.xlsx")
        schedule_df.to_excel(out_file, index=False)
    else:
        print("No output path found. Skipping save.")