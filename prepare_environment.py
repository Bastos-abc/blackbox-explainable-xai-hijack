#!/usr/bin/python3
from multiprocessing import Pool
import os
from datetime import datetime, timedelta, timezone

""" This code downloads files with information needed to run DFOH and compute the features to train the 
    AI model to test and compare  models with fewer features, the work is described in the paper 
    #Abrindo a Caixa-Preta -- Aplicando IA Explicável para Aprimorar a Detecção de Sequestros de Prefixo#
"""

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


# Calculation to increase or decrease a date by N days
def date_plus(start_date: str, plus=1):
    """
    :param start_date: "YYYY-MM-DD" base date
    :param plus: How many days to increase or decrease (-x)
    :return: "YYYY-MM-DD"
    """
    s_day = start_date.split('-')[2]
    s_m = start_date.split('-')[1]
    s_y = start_date.split('-')[0]
    begin = datetime(int(s_y), int(s_m), int(s_day), 0, 0, 0, tzinfo=timezone.utc)
    begin = begin + timedelta(days=plus)
    year = begin.year
    month = begin.month
    day = begin.day
    n_day = str(year) + '-' + str(month) + '-' + str(day)
    return n_day


# Download the necessaries files to run DFOH
#   The codes used to download files were made by the researchers who developed DFOH (https://dfoh.uclouvain.be/)
#   (with some adjusts)
#   All files created will storage in first dataset folder (db_m1)
def download_files(days_d: list, db_dir: str, n_threads: int, max_work: int):
    """
     Download information from collectors, PeeringDB and CAIDA to create topological features and peering features.
    :param days_d: list with dates
    :param db_dir: folder used to storage dataset information
    :param n_threads: how many threads can be running
    :param max_work: how many parallel downloads will be executed
    :return: None
    """
    local_folder = os.getcwd()
    os.chdir('./db/main/')
    print("*******************Downloading necessary files from collectors**************")
    args = []
    for d in days_d:
        cmd = "python3 collector.py "
        cmd += " --date '" + d + "T00:00:00'"
        cmd += " --nb_vps 200 "
        cmd += " --db_dir " + db_dir
        cmd += " --max_workers " + n_threads
        cmd += " --max_workers_rib " + n_threads
        args.append([cmd])
    with Pool(processes=2) as th_pool:
        th_pool.starmap(os.system, args, )

    print("*********************Downloading necessary files (Topology)*************************")
    args = []
    for d in days_d:
        cmd = "python3 get_topology.py "
        cmd += " --date '" + d + "T00:00:00'"
        cmd += " --db_dir " + db_dir
        args.append([cmd])
    with Pool(processes=int(n_threads)) as th_pool:
        th_pool.starmap(os.system, args, )
    os.chdir(local_folder)

    # 'merged-topology' files
    print("*****************************Creating merger files**********************************")
    args = []
    cmd = "python3 merger.py"
    cmd += " --date '" + days_d[0] + "T00:00:00'"
    cmd += " --db_dir " + db_dir
    cmd += " --max_workers " + max_work
    cmd += " --date_end '" + date_plus(days_d[-1]) + "T00:00:00'"
    args.append([cmd])
    os.chdir('./newedge/main/')
    os.system(cmd)
    os.chdir(local_folder)


# Create files with new edges observed in a day
# The code made by the researchers who developed DFOH (https://dfoh.uclouvain.be/)
# (with some adjusts)
def create_new_edges(days_d: list, db_dir: str, n_threads: int, max_work: int):
    """
    Create new edge files
    :param days_d: list with dates
    :param db_dir: folder used to storage dataset information
    :param n_threads: how many threads can be running
    :param max_work: how many parallel downloads will be executed
    :return: None
    """
    # 'new_edges'
    print("*****************************Creating new_edges files**********************************")
    args = []
    for d in days_d[1:]:
        cmd = "python3 orchestrator.py"
        cmd += " --date '" + d + "T00:00:00'"
        cmd += " --db_dir " + db_dir
        cmd += " --max_workers " + max_work
        cmd += " --nb_vps 200 "
        args.append([cmd])
    os.chdir('./newedge/main/')
    with Pool(processes=int(n_threads)) as th_pool:
        th_pool.starmap(os.system, args, )
    os.chdir(local_folder)


# Create sampling to train the AI model
# The code made by the researchers who developed DFOH (https://dfoh.uclouvain.be/)
# (with some adjusts)
def create_samplig(date: str, size: int, db_dir: str):
    """
    Create sampling to train the model
    :param date: "YYYY-MM-DD"
    :param size: How many samples by class
    :param db_dir: folder used to storage dataset information
    :return:
    """
    print("*****************************Creating samplings to training**********************************")
    cmd = "python3 sampler.py"
    cmd += " --date {}".format(date)
    cmd += " --size {}".format(str(size))
    cmd += " --db_dir {}".format(db_dir)
    cmd += " --output {}".format(db_dir)
    print("Start -> " + cmd)
    os.system(cmd)
    print("Finish -> " + cmd)
    return None

# All files created and are common with all datasets will be generated once and a symbolic link will be
# created to other dataset folders
def create_environment_folders(db_tests: list()):
    """
    :param db_tests: list with dataset folders
    :return: None
    """
    for db in db_tests:
        if not os.path.isdir(db):
            os.mkdir(db)

    folders = ['cone', 'full_topology', 'irr', 'merged_topology', 'new_edge', 'paths', 'peeringdb', 'prefixes',
               'sampling', 'sampling_cluster', 'topology']
    cmd = 'ln -s {} {}/'
    for f in folders:
        for db in db_tests[1:]:
            src = '{}/{}'.format(db_tests[0],f)
            if not os.path.isdir(src):
                os.mkdir(src)
            cmd_t = cmd.format(src, db)
            os.system(cmd_t)


# Create files with features needs to training the model
def create_features(date: str, db_tests: list):
    """
    :param date: "YYYY-MM-DD"
    :param db_tests: list with dataset folders
    :return: None
    """
    cmd = ('time -p -a -o {}/{}.log python3 run_test.py --date {} --db_dir {} '
           '--logfile {} --only_prepare {} --new_edges {}')
    log_folder = './log_files_prepare'
    only_prepare = True
    newedge = False
    if not os.path.isdir(log_folder):
        os.mkdir(log_folder)
    logfile = '{}/tempos_de_execucao.log'.format(log_folder)
    for db in db_tests:
        cmd_t = cmd.format(log_folder, 'test_{}'.format(date), date, db, logfile, only_prepare, newedge)
        os.system(cmd_t)



if __name__ == '__main__':
    first_test_date = "2023-12-01"
    end_test_date = "2023-12-20"
    days_s_ne = dates(first_test_date, end_test_date)
    # First day for download files
    first_date_d = date_plus(first_test_date, -301)
    # First day for create features
    first_date_f = date_plus(first_test_date, -61)
    # Days for download files
    days_d = dates(first_date_d, end_test_date)
    # Days for create sampling
    days_s = dates(date_plus(first_date_f,-60), end_test_date)
    # Days for create features
    days_f = dates(first_date_f, date_plus(first_test_date,-1))
    max_work = "13"
    n_threads = "5"
    local_folder = os.getcwd()
    # Folder to save files when execute with all features
    root = "/home/dfoh_nv/"
    db_tests = [root + "db_m1", root + "db_m4", root + "db_m5"]
    # db_m1 = Model with 28 features (original model)
    # db_m4 = Model with top 11 features
    # db_m5 = Model with top 5 features

    download_files(days_d, db_tests[0], n_threads, max_work)
    create_new_edges(days_d, db_tests[0], n_threads, max_work)
    print("**********************Creating sampling***********************************")
    args = []

    for d in days_s:
        args.append([d, 1000, db_tests[0]])

    th = len(args) if len(args) < int(n_threads) else int(n_threads)

    with Pool(processes=th) as th_pool:
        th_pool.starmap(create_samplig, args,)

    create_environment_folders(db_tests)
    args = []
    for date in days_f:
        args.append([date, db_tests])

    th = len(args) if len(args) < int(n_threads) else int(n_threads)
    with Pool(processes=int(th)) as th_pool:
        th_pool.starmap(create_features, args, )
