import os
from colorama import Fore, Style
from datetime import datetime, timedelta, timezone
from time import time
import click


'''
Code used to generate the model training and the inference of the new edges with the original set of features 
and also through some features selected for testing (m4 with 11 features and m5 with 5 features)
'''

def print_prefix():
    return Fore.GREEN + Style.BRIGHT + "[run_daily.py]: " + Style.NORMAL


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
def get_files(date: str, db_dir: str):
    """
    Download files needed
    :param date: "YYYY-MM-DD"
    :param db_dir: dataset folder
    :return: None
    """
    local_folder = os.getcwd()
    os.chdir('./db/main/')
    print("*******************Downloading necessary files**************")
    cmd = "python3 collector.py "
    cmd += " --date '" + date + "T00:00:00'"
    cmd += " --db_dir " + db_dir
    cmd += " --max_workers 4"  # max_work
    cmd += " --max_workers_rib 4"  # max_work
    cmd += " --nb_vps 200"  # number of vps
    os.system(cmd)

    print("*********************Downloading necessary files (Topology)*************************")
    cmd = "python3 get_topology.py "
    cmd += " --date '" + date + "T00:00:00'"
    cmd += " --db_dir " + db_dir
    os.system(cmd)
    os.chdir(local_folder)

    cmd = "python3 merger.py"
    cmd += " --date '" + date + "T00:00:00'"
    cmd += " --db_dir " + db_dir
    cmd += " --max_workers 4" # max_work
    cmd += " --date_end '" + date_plus(date) + "T00:00:00'"
    #cmd += " --override=True"
    os.chdir('./newedge/main/')
    os.system(cmd)
    os.chdir(local_folder)


# Create sampling to train the AI model
# The code made by the researchers who developed DFOH (https://dfoh.uclouvain.be/)
# (with some adjusts)
def create_sampling_files(date, db_dir):
    """
    Create sampling to train the model
    :param date: "YYYY-MM-DD"
    :param db_dir: folder used to storage dataset information
    :return:
    """
    max_work = '15'
    print("**********************Creating sampling***********************************")
    cmd = "python3 sampler.py"
    cmd += " --date '" + date + "'"
    cmd += " --db_dir " + db_dir
    cmd += " --output " + db_dir
    cmd += " --nb_threads " + max_work
    cmd += " --size=1000"
    cmd += " --k_pos=0.75"
    cmd += " --k_neg=1"
    print(cmd)
    os.system(cmd)


# Create AS path pattern features to train the AI model
# The code made by the researchers who developed DFOH (https://dfoh.uclouvain.be/)
# (with some adjusts)
def feat_aspth(date:str, db_dir:str, asp_feat:list):
    """
    :param date: "YYYY-MM-DD"
    :param db_dir: folder used to storage dataset information
    :param asp_feat: list with features that will be calculated
    :return:
    """
    import aspath_feat
    aspath_feat.run_orchestrator(date=date, db_dir=db_dir, daily_sampling=1,
                                 nbdays=60, override=0, nb_threads=6, metrics=asp_feat)


# Create bidirectionality features to train the AI model
# The code made by the researchers who developed DFOH (https://dfoh.uclouvain.be/)
# (with some adjusts)
def feat_bidi(date: str, db_dir: str, daily_sampling: bool=True, method: str="clusters",
              override: bool=False, bidi_feat: list=["bidi", "nb_vps"]):
    """
    :param date: "YYYY-MM-DD"
    :param db_dir: folder used to storage dataset information
    :param daily_sampling: create daily sampling
    :param method:  default method is clusters
    :param override: Override exists files
    :param bidi_feat: list with features that will be calculated
    :return:
    """
    import bidirectionality as bidi
    print('feat_bidi', bidi_feat)
    bidi.launch_orchestrator(date=date, db_dir=db_dir, daily_sampling=daily_sampling,
                             method=method, override=override, feat=bidi_feat)


# Create peering features to train the AI model
# The code made by the researchers who developed DFOH (https://dfoh.uclouvain.be/)
# (with some adjusts)
def feat_peering(date: str, db_dir: str, daily_sampling: bool=True, method: str="clusters", override: bool=False,
                 feat=["country_dist", "facility_fac_dist", "facility_country_dist", "facility_cities_dist", "ixp_dist"]):
    """
    :param date: "YYYY-MM-DD"
    :param db_dir: folder used to storage dataset information
    :param daily_sampling: create daily sampling
    :param method:  default method is clusters
    :param override: Override exists files
    :param feat: list with features that will be calculated
    :return:
    """
    import peering
    peering.launch_orchestrator(date=date, db_dir=db_dir, daily_sampling=daily_sampling,
                                method=method, override=override, feat=feat)

# Create topological features to train the AI model
# The code made by the researchers who developed DFOH (https://dfoh.uclouvain.be/)
# (with some adjusts)
def feat_topo(date: str, db_dir: str, daily_sampling: int=1, override: int=0, nb_threads: int=5,
              topo_feat_exclude: str=''):
    """
    :param date: "YYYY-MM-DD"
    :param db_dir: folder used to storage dataset information
    :param daily_sampling: create daily sampling
    :param override: Override exists files
    :param nb_threads: number of threads to calculate features
    :param topo_feat_exclude: list with features that will be not calculated (string format separated by ',')
    :return:
    """
    import topo_feat
    topo_feat.run_orchestrator(date=date, db_dir=db_dir, daily_sampling=daily_sampling,
                               overide=override, nb_threads=nb_threads, feat_exclude=topo_feat_exclude)


# Create all features needed to train the AI model
def run_features(date: str, db_dir: str, aspath_feat: list, bidi_feat: list, peer_feat: list, topo_feat_exclude: str,
                 log):
    """
    Create all features needed to train the AI model
    :param date: "YYYY-MM-DD"
    :param db_dir: folder used to storage dataset information
    :param aspath_feat: list with features that will be calculated (AS path pattern)
    :param bidi_feat: list with features that will be calculated (bidirectionality)
    :param peer_feat: list with features that will be calculated (peering)
    :param topo_feat_exclude: list with features that will be not calculated (string format separated by ',') (Topological)
    :param log: file to save execution information (open file to write)
    :return:
    """
    print(print_prefix() + "START: {} {}".format(date, 'features'))

    # Run the AS-path features
    print("Features - AS path")
    start = time()
    feat_aspth(date=date, db_dir=db_dir, asp_feat=aspath_feat)
    print("Create AS_path features took {:.4f} seconds".format(time() - start), file=log)

    # Run Bidirectionnality features
    print("Features - Bidi")
    start = time()
    feat_bidi(date=date, db_dir=db_dir, daily_sampling=True,
              method="clusters", override=False, bidi_feat=bidi_feat)
    print("Create bidi features took {:.4f} seconds".format(time() - start), file=log)

    # Run PeeringDB features
    print("Features - Peering")
    start = time()
    feat_peering(date=date, db_dir=db_dir, daily_sampling=True,
                 method="clusters", override=False, feat=peer_feat)
    print("Create peering features took {:.4f} seconds".format(time() - start), file=log)

    # Run Topological features
    print("Features - Topological")
    start = time()
    feat_topo(date=date, db_dir=db_dir, daily_sampling=1, override=0,
              nb_threads=10, topo_feat_exclude=topo_feat_exclude)
    print("Create topological features took {:.4f} seconds".format(time() - start), file=log)

# Create files with new edges
# The code made by the researchers who developed DFOH (https://dfoh.uclouvain.be/)
# (with some adjusts)
def run_new_edges(date, db_dir):
    """
    Create files with new edges observed in the day
    :param date: "YYYY-MM-DD"
    :param db_dir: folder used to storage dataset information
    :return: None
    """
    import newedge
    print(print_prefix() + "START: {} {}".format(date, 'new_edge'))

    # First check whether all the data is there.

    newedge.compute_new_edge(date='{}T00:00:00'.format(date), db_dir=db_dir, nb_vps=200,
                             max_workers=10)


# Run broker to train model and to classifier new edges
# The code made by the researchers who developed DFOH (https://dfoh.uclouvain.be/)
# (with some adjusts)
def run_broker_inference(date: str, db_dir: str, aspath_feat: list, bidi_feat: list, peer_feat: list, topo_feat_exclude: str):
    """
    :param date: "YYYY-MM-DD"
    :param db_dir: folder used to storage dataset information
    :param aspath_feat: list with features that will be calculated (AS path pattern)
    :param bidi_feat: list with features that will be calculated (bidirectionality)
    :param peer_feat: list with features that will be calculated (peering)
    :param topo_feat_exclude: list with features that will be not calculated (string format separated by ',') (Topological)
    :return:
    """
    from broker import RequestBroker
    print(print_prefix() + "START: {} {}".format(date, 'inference'))
    cases = db_dir + '/cases'
    if not os.path.isdir(cases):
        os.mkdir(cases)


    # Parameters of the broker
    fn = "{}/new_edge/{}.txt".format(db_dir, date)
    nb_days_training = 60
    print("Broker")
    Broker = RequestBroker(date, db_dir, aspath_feat, bidi_feat, peer_feat, topo_feat_exclude, nb_days_training)
    outfile = "{}/{}.tmp".format(cases, date)
    if os.path.exists(fn) and not os.path.exists(outfile):
        Broker.process_request(fn, outfile=outfile)

    print(print_prefix() + "DONE: {} {}".format(date, 'inference'))


# Run parser to create readable files based on new edge inference files
# The code made by the researchers who developed DFOH (https://dfoh.uclouvain.be/)
# (with some adjusts)
def run_parser(date: str, db_dir: str):
    """
    Create readable files based on new edge inference files
    :param date: "YYYY-MM-DD"
    :param db_dir: folder used to storage dataset information
    :return: None
    """
    from parse.parse import launch_parser
    print(print_prefix() + "START: {} {}".format(date, 'parser'))
    cases_dirs = ['cases']
    for cd in cases_dirs:
        parser_file = '{}/{}/{}'.format(db_dir, cd, date)
        if not os.path.exists(parser_file):
            launch_parser(db_dir, date, cd)


@click.command()
@click.option('--date', help='Date for which to compute peeringdb features, in the following format "YYYY-MM-DD".', type=str)
@click.option('--db_dir', help='Database directory (Needs to end with m1, m4 or m5)".', type=str)
@click.option('--logfile', help='log file".', type=str)
@click.option('--only_prepare', default=False, help='Only create features".', type=bool)
@click.option('--new_edges', default=False, help='If it needs to create new_edges files".', type=bool)
@click.option('--download_files', default=False, help='If it needs to download files".', type=bool)
@click.option('--train_feat', default=True, help='If it needs to download files".', type=bool)
@click.option('--sampling', default=True, help='If it needs to create sampling files to train".', type=bool)
def launch_test(date, db_dir, logfile, only_prepare, new_edges, download_files, train_feat, sampling):
    log = open(logfile,'a')
    all_features = ["degree", "cone", "cone_degree", "bidi", "nb_vps", "country_dist", "facility_fac_dist",
                    "facility_country_dist", "facility_cities_dist", "ixp_dist", "degree_centrality_as1",
                    "degree_centrality_as2", "average_neighbor_degree_as1", "average_neighbor_degree_as2",
                    "triangles_as1", "triangles_as2", "clustering_as1", "clustering_as2", "eccentricity_as1",
                    "eccentricity_as2", "harmonic_centrality_as1", "harmonic_centrality_as2",
                    "closeness_centrality_as1", "closeness_centrality_as2", "shortest_path", "jaccard", "adamic_adar",
                    "preferential_attachement"]

    #topo_feat = ["degree_centrality_as1", "degree_centrality_as2", "average_neighbor_degree_as1",
    #             "average_neighbor_degree_as2", "triangles_as1", "triangles_as2", "clustering_as1", "clustering_as2",
    #             "eccentricity_as1", "eccentricity_as2", "harmonic_centrality_as1", "harmonic_centrality_as2",
    #             "closeness_centrality_as1", "closeness_centrality_as2", "shortest_path", "jaccard", "adamic_adar",
    #             "preferential_attachement"]

    test_type = db_dir.split('_')[-1].strip('/')
    if test_type == 'm1':
        print("Starting normal execution for day {}".format(date), file=log)
        topo_feat_exclude = []
        aspath_feat = ["degree", "cone", "cone_degree"]
        bidi_feat = ["bidi", "nb_vps"]
        peer_feat = ["country_dist", "facility_fac_dist", "facility_country_dist", "facility_cities_dist", "ixp_dist"]

    elif test_type == 'm4':
        print("Starting execution with M4 features for day {}".format(date), file=log)
        topo_feat_exclude = ["degree_centrality", "average_neighbor_degree",
                             "eccentricity", "harmonic_centrality", "closeness_centrality", "shortest_path", "jaccard",
                             "adamic_adar"]
        aspath_feat = ["degree", "cone_degree"]
        bidi_feat = ["bidi",  "nb_vps"]
        peer_feat = ["country_dist", "ixp_dist"]

    elif test_type == 'm5':
        print("Starting execution with M4 features for day {}".format(date), file=log)
        topo_feat_exclude = ["degree_centrality", "average_neighbor_degree",
                             "eccentricity", "harmonic_centrality", "closeness_centrality", "shortest_path", "jaccard",
                             "adamic_adar", "triangles", "preferential_attachement"]
        aspath_feat = ["cone_degree"]
        bidi_feat = ["bidi"]
        peer_feat = ["country_dist"]

    else:
        print('Test type not found, check database folder name:', test_type)
        exit(1)

    feat_exclude = "pagerank,eigenvector_centrality,square_clustering,number_of_cliques,simrank_similarity"
    for f in topo_feat_exclude:
        feat_exclude += ",{}".format(f)
    # Compute all the features values.
    if download_files:
        get_files(date, db_dir)
    if sampling:
        create_sampling_files(date, db_dir)
    if train_feat:
        print("run_features")
        start = time()
        run_features(date, db_dir, aspath_feat, bidi_feat, peer_feat, feat_exclude, log)
        print("Create features took {:.4f} seconds".format(time() - start), file=log)
        log.close()
    # Find all the new edges (legitimate and suspicious).
    if new_edges:
        print("run_new_edges")
        start = time()
        run_new_edges(date, db_dir)
        log = open(logfile, 'a')
        print("Run new edges took {:.4f} seconds".format(time() - start), file=log)
        log.close()

    if not only_prepare:
        # Run DFOH on the new edge cases.
        print('###############Broker')
        start = time()
        print('Calc new edge sampling features')
        run_broker_inference(date, db_dir, aspath_feat, bidi_feat, peer_feat, feat_exclude)
        log = open(logfile, 'a')
        print("Run broker inference took {:.4f} seconds".format(time() - start), file=log)
        log.close()

        # Parse the results and infer the suspicious cases.
        print('###############Parser')
        start = time()
        log = open(logfile, 'a')
        run_parser(date, db_dir)
        print("Run parser took {:.4f} seconds".format(time() - start), file=log)

        log.close()

if __name__ == "__main__":
    launch_test()

