from datetime import datetime, timedelta, timezone
import os
from time import time


# Create a list with a sequence of dates
def dates(start_date: str, end_date: str):
    """
    :param start_date: "YYYY-MM-DD" - first date
    :param end_date: "YYYY-MM-DD" - last date
    :return: list with dates (str)
    """
    days = []
    s_day = start_date.split('-')[2]
    s_m = start_date.split('-')[1]
    s_y = start_date.split('-')[0]
    e_day = end_date.split('-')[2]
    e_m = end_date.split('-')[1]
    e_y = end_date.split('-')[0]
    begin = datetime(int(s_y), int(s_m), int(s_day), 0, 0, 0, tzinfo=timezone.utc)
    end = datetime(int(e_y), int(e_m), int(e_day), 0, 0, 0, tzinfo=timezone.utc)
    while begin <= end:
        year = begin.year
        month = str(begin.month)
        if len(month)<2 :
            month = '0' + month
        day = str(begin.day)
        if len(day) <2 :
            day = '0' + day
        days.append(str(year)+'-'+ month +'-'+ day)
        begin = begin + timedelta(days=1)

    return days

if __name__ == "__main__":
    # Previous files need to be created with prepare_envionmet.py for the period
    days = dates("2023-12-01", "2023-12-20") #Select first and last day, a range to execute
    cmd = ('time -p -a -o {}/{}.log python3 run_test.py --date {} --db_dir {} --logfile {} --only_prepare {} --new_edges {} '
           '--download_files {} --sampling {} --train_feat {}')
    db_folders = ['/home/dfoh_nv/db_m1/', '/home/dfoh_nv/db_m4/', '/home/dfoh_nv/db_m5/']
    log_folder = './log_run_in_loop' #Folder to save log files
    # All below parameters that its set with 'False' it's because to not interfere in the time execution measurement
    # They need to be created with prepare_envionmet.py previously
    only_prepare = False #If True, It'll not run broker and parser
    train_feat = True #If True, It'll create all features to train ML (for one day, not all needed)
    down_files = False #If True, It'll download all necessary files if they don't exist (for one day, not all needed)
    edges = False #If True, It'll run new_edges (for one day)
    sampling = False #If True, It'll create sampling files if they don't exist (for one day, not all needed)
    if not os.path.isdir(log_folder):
        os.mkdir(log_folder)
    for i in range(1):
        for date in days:
            for db in db_folders:
                start = time()
                logfile = '{}/execution_time.log'.format(log_folder)
                log = open(logfile, 'a')
                print("#################################################", file=log)
                test_type = db.split('_')[-1].strip('/')
                print("Starting {} execution for day {}".format(test_type, date), file=log)
                cmd_n = cmd.format(log_folder, '{}_{}'.format(test_type, date), date, db, logfile,
                                   only_prepare, edges, down_files, sampling, train_feat)
                log.close()
                os.system(cmd_n)
                log = open(logfile, 'a')
                print("{} execution for day {} took {:.4f} seconds".format(test_type, date, time() - start), file=log)
                log.close()
