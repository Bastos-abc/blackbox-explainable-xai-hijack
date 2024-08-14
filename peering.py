from concurrent.futures import ProcessPoolExecutor
import os
import sys
from datetime import datetime
import pandas as pd
#import click
#from multiprocessing import Process

from colorama import Fore
from colorama import Style
from colorama import init
init(autoreset=True)

from utils.country import CountryFeaturesComputation
from utils.facility import FacilityFeaturesComputation
from utils.ixp import IXPFeaturesComputation
from utils.cosine import CosineDistance
from time import mktime

class Orchestrator:
    def __init__(self, method, feat, db_dir: str=None):
        self.db_dir = db_dir+'/'
        self.method = method
        self.feats = feat
        if not os.path.isdir(self.db_dir):
            print('Database does not exist: exit.', file=sys.stderr)
            sys.exit(0)
        if not os.path.isdir(self.db_dir+'/features'):
            os.mkdir(self.db_dir+'/features')
        if not os.path.isdir(self.db_dir+'/features/tmp_peeringdb'):
            os.mkdir(self.db_dir+'/features/tmp_peeringdb')
        if not os.path.isdir(self.db_dir+'/features/positive/peeringdb_{}'.format(method)):
            os.mkdir(self.db_dir+'/features/positive/peeringdb_{}'.format(method))
        if not os.path.isdir(self.db_dir+'/features/negative/peeringdb/'):
            os.mkdir(self.db_dir+'/features/negative/peeringdb/')

        self.country_features_obj = None

    def print_prefix(self):
        return Fore.GREEN+Style.BRIGHT+"[peering.py]: "+Style.NORMAL

    def compute_country_feature_helper(self, topo_file, country_file, outfile_country):
        CountryFeaturesComputation(topo_file, country_file).construct_features(outfile_country)

    def compute_facility_features_helper(self, topo_file, facility_file, outfile_facility_fac, outfile_facility_country, outfile_facility_cities, date):
        ffc = FacilityFeaturesComputation(topo_file, facility_file)
        if "facility_fac_dist" in self.feats:
            ffc.construct_features(ffc.node_to_facilities, ffc.mapping_facilities, outfile_facility_fac)
        if "facility_country_dist" in self.feats:
            ffc.construct_features(ffc.node_to_countries, ffc.mapping_countries, outfile_facility_country)
        if "facility_cities_dist" in self.feats:
            ffc.construct_features(ffc.node_to_cities, ffc.mapping_cities, outfile_facility_cities)

    def compute_ixp_feature_helper(self, topo_file, ixp_file, outfile_ixp):
        IXPFeaturesComputation(topo_file, ixp_file).construct_features(outfile=outfile_ixp)
        
    def compute_nodes_features(self, ts: str=None, override: bool=False):
        date = datetime.strptime(ts, "%Y-%m-%d")
        topo_file = self.db_dir+'/merged_topology/'+date.strftime("%Y-%m-%d")+".txt"
        country_file = self.db_dir+'peeringdb/'+date.strftime("%Y-%m-%d")+"_country.txt"
        facility_file = self.db_dir+'peeringdb/'+date.strftime("%Y-%m-%d")+"_facility.txt"
        ixp_file = self.db_dir+'peeringdb/'+date.strftime("%Y-%m-%d")+"_ixp.txt"

        plist = []

        #Compute the country features.
        if "country_dist" in self.feats:
            print(self.print_prefix()+"Computing nodes' country features for: {}".format(date), file=sys.stderr)
            outfile_country = self.db_dir+'/features/tmp_peeringdb/'+date.strftime("%Y-%m-%d")+"_country.pkl"
            if not os.path.isfile(outfile_country) or override:
                # pcountry = Process(target=self.compute_country_feature_helper, args=(topo_file, country_file, outfile_country,))
                # plist.append(pcountry)
                self.compute_country_feature_helper(topo_file, country_file, outfile_country)

        # Compute the facility features.
        if "facility_fac_dist" in self.feats or "facility_country_dist" in self.feats or "facility_cities_dist" in self.feats:
            print(self.print_prefix()+"Computing nodes' facility features for: {}".format(date), file=sys.stderr)
            outfile_facility_fac = self.db_dir+'/features/tmp_peeringdb/'+date.strftime("%Y-%m-%d")+"_facility_fac.pkl"
            outfile_facility_country = self.db_dir+'/features/tmp_peeringdb/'+date.strftime("%Y-%m-%d")+"_facility_country.pkl"
            outfile_facility_cities = self.db_dir+'/features/tmp_peeringdb/'+date.strftime("%Y-%m-%d")+"_facility_cities.pkl"
            if (not os.path.isfile(outfile_facility_fac) or \
                not os.path.isfile(outfile_facility_country) or \
                not os.path.isfile(outfile_facility_cities) or \
                override):

                self.compute_facility_features_helper(topo_file, facility_file, outfile_facility_fac, outfile_facility_country, outfile_facility_cities, date)

        # Compute the ixp features.
        if "ixp_dist" in self.feats:
            print (self.print_prefix()+"Computing nodes' ixp features for: {}".format(date), file=sys.stderr)
            outfile_ixp = self.db_dir+'/features/tmp_peeringdb/'+date.strftime("%Y-%m-%d")+"_ixp.pkl"
            if not os.path.isfile(outfile_ixp) or override:
                self.compute_ixp_feature_helper(topo_file, ixp_file, outfile_ixp)


    def compute_edge_features_daily_sampling(self, ts: str=None, override: bool=False):
        date = datetime.strptime(ts, "%Y-%m-%d")
        print (self.print_prefix()+"Computing edges' features for: {}".format(date), file=sys.stderr)

        # Stop if the outfiles exist already if of override is False.
        outfile_positive = self.db_dir+'/features/positive/peeringdb_{}/'.format(self.method)+date.strftime("%Y-%m-%d")+"_positive.txt"
        outfile_negative = self.db_dir+'/features/negative/peeringdb/'+date.strftime("%Y-%m-%d")+"_negative.txt"
        if (os.path.isfile(outfile_positive) and os.path.isfile(outfile_negative) and not override):
            return

        topo_file = self.db_dir+'/merged_topology/'+date.strftime("%Y-%m-%d")+".txt"
        sample_file_positive = self.db_dir+'/sampling/positive/sampling_{}/'.format(self.method)+date.strftime("%Y-%m-%d")+"_positive.txt"
        sample_file_negative = self.db_dir+'/sampling/negative/sampling/'+date.strftime("%Y-%m-%d")+"_negative.txt"
        # Stop if the sample files are not available.
        if (not os.path.isfile(sample_file_positive) or not os.path.isfile(sample_file_negative)):
            print(self.print_prefix()+"Sample files not available. Please do the sampling first.".format(date), file=sys.stderr)
            return 
        
        # Parsing the sampled AS links, and creating the dataframe for positive and negative cases.
        positive_links = []
        self.df_positive = pd.DataFrame(columns=['as1', 'as2'])
        with open(sample_file_positive, 'r') as fd:
            for line in fd.readlines():
                linetab = line.split(',')[0].split(' ')
                if int(linetab[0]) > int(linetab[1]):
                    as1 = linetab[1]
                    as2 = linetab[0]
                else:
                    as1 = linetab[0]
                    as2 = linetab[1]
                positive_links.append((int(as1), int(as2)))
                self.df_positive.loc[len(self.df_positive)] = [int(as1), int(as2)]

        negative_links = []
        self.df_negative = pd.DataFrame(columns=['as1', 'as2'])
        with open(sample_file_negative, 'r') as fd:
            for line in fd.readlines():
                linetab = line.split(',')[0].split(' ')
                if int(linetab[0]) > int(linetab[1]):
                    as1 = linetab[1]
                    as2 = linetab[0]
                else:
                    as1 = linetab[0]
                    as2 = linetab[1]
                negative_links.append((int(as1), int(as2)))
                self.df_negative.loc[len(self.df_negative)] = [int(as1), int(as2)]            

        # Compute cosine distance for country feature.
        country_features = self.db_dir+'/features/tmp_peeringdb/'+date.strftime("%Y-%m-%d")+"_country.pkl"
        if 'country_dist' in self.feats:
            cd = CosineDistance(topo_file, country_features)
            self.df_positive = self.df_positive.merge(cd.compute_distance(positive_links), how='left').rename(columns = {'distance':'country_dist'})
            self.df_negative = self.df_negative.merge(cd.compute_distance(negative_links), how='left').rename(columns = {'distance':'country_dist'})

        # Compute cosine distance for facility features.
        outfile_facility_fac = self.db_dir+'/features/tmp_peeringdb/'+date.strftime("%Y-%m-%d")+"_facility_fac.pkl"
        outfile_facility_country = self.db_dir+'/features/tmp_peeringdb/'+date.strftime("%Y-%m-%d")+"_facility_country.pkl"
        outfile_facility_cities = self.db_dir+'/features/tmp_peeringdb/'+date.strftime("%Y-%m-%d")+"_facility_cities.pkl"
        if 'facility_fac_dist' in self.feats:
            cd = CosineDistance(topo_file, outfile_facility_fac)
            self.df_positive = self.df_positive.merge(cd.compute_distance(positive_links), how='left').rename(columns = {'distance':'facility_fac_dist'})
            self.df_negative = self.df_negative.merge(cd.compute_distance(negative_links), how='left').rename(columns = {'distance':'facility_fac_dist'})
        if 'facility_country_dist' in self.feats:
            cd = CosineDistance(topo_file, outfile_facility_country)
            self.df_positive = self.df_positive.merge(cd.compute_distance(positive_links), how='left').rename(columns = {'distance':'facility_country_dist'})
            self.df_negative = self.df_negative.merge(cd.compute_distance(negative_links), how='left').rename(columns = {'distance':'facility_country_dist'})
        if 'facility_cities_dist' in self.feats:
            cd = CosineDistance(topo_file, outfile_facility_cities)
            self.df_positive = self.df_positive.merge(cd.compute_distance(positive_links), how='left').rename(columns = {'distance':'facility_cities_dist'})
            self.df_negative = self.df_negative.merge(cd.compute_distance(negative_links), how='left').rename(columns = {'distance':'facility_cities_dist'})

         # Compute cosine distance for country feature.
        if 'ixp_dist' in self.feats:
            ixp_features = self.db_dir+'/features/tmp_peeringdb/'+date.strftime("%Y-%m-%d")+"_ixp.pkl"
            cd = CosineDistance(topo_file, ixp_features)
            self.df_positive = self.df_positive.merge(cd.compute_distance(positive_links), how='left').rename(columns = {'distance':'ixp_dist'})
            self.df_negative = self.df_negative.merge(cd.compute_distance(negative_links), how='left').rename(columns = {'distance':'ixp_dist'})

        # Writing the resulting dataframe.
        self.df_positive.to_csv(outfile_positive, sep=' ', index=False)
        self.df_negative.to_csv(outfile_negative, sep=' ', index=False)


    def compute_edge_features_links(self, ts: str, outfile, links=set, return_df: bool=False):
        date = datetime.strptime(ts, "%Y-%m-%d")
        print(self.print_prefix()+"Computing edges' features for: {}".format(date), file=sys.stderr)

        df = pd.DataFrame(columns=['as1', 'as2'])

        for as1, as2, in links:
            df.loc[len(df)] = [as1, as2]

        topo_file = self.db_dir+'/merged_topology/'+date.strftime("%Y-%m-%d")+".txt"

        # Compute cosine distance for country feature.
        if "country_dist" in self.feats:
            country_features = self.db_dir+'/features/tmp_peeringdb/'+date.strftime("%Y-%m-%d")+"_country.pkl"
            cd = CosineDistance(topo_file, country_features)
            df = df.merge(cd.compute_distance(links), how='left').rename(columns = {'distance':'country_dist'})

        # Compute cosine distance for facility features.
        outfile_facility_fac = self.db_dir+'/features/tmp_peeringdb/'+date.strftime("%Y-%m-%d")+"_facility_fac.pkl"
        outfile_facility_country = self.db_dir+'/features/tmp_peeringdb/'+date.strftime("%Y-%m-%d")+"_facility_country.pkl"
        outfile_facility_cities = self.db_dir+'/features/tmp_peeringdb/'+date.strftime("%Y-%m-%d")+"_facility_cities.pkl"
        if "facility_fac_dist" in self.feats:
            cd = CosineDistance(topo_file, outfile_facility_fac)
            df = df.merge(cd.compute_distance(links), how='left').rename(columns = {'distance':'facility_fac_dist'})
        if "facility_country_dist" in self.feats:
            cd = CosineDistance(topo_file, outfile_facility_country)
            df = df.merge(cd.compute_distance(links), how='left').rename(columns = {'distance':'facility_country_dist'})
        if "facility_cities_dist" in self.feats:
            cd = CosineDistance(topo_file, outfile_facility_cities)
            df = df.merge(cd.compute_distance(links), how='left').rename(columns = {'distance':'facility_cities_dist'})

        # Compute cosine distance for country feature.
        if "ixp_dist" in self.feats:
            ixp_features = self.db_dir+'/features/tmp_peeringdb/'+date.strftime("%Y-%m-%d")+"_ixp.pkl"
            cd = CosineDistance(topo_file, ixp_features)
            df = df.merge(cd.compute_distance(links), how='left').rename(columns = {'distance':'ixp_dist'})

        # Writing the resulting dataframe in stdout.
        if return_df:
            return df
        elif outfile:
            with open(outfile, "w") as f:
                df.to_csv(f, sep=' ', index=False)
        else:
            df.to_csv(sys.stdout, sep=' ', index=False)

    def clean_files(self, ts: str=None):
        # This function removes all the temporary files (pickle files).
        date = datetime.strptime(ts, "%Y-%m-%d")
        print (self.print_prefix()+"Cleaning up pickle files for: {}".format(date), file=sys.stderr)

        outfile_country = self.db_dir+'/features/tmp_peeringdb/'+date.strftime("%Y-%m-%d")+"_country.pkl"
        if os.path.isfile(outfile_country):
            os.remove(outfile_country) 

        outfile_facility_fac = self.db_dir+'/features/tmp_peeringdb/'+date.strftime("%Y-%m-%d")+"_facility_fac.pkl"
        if os.path.isfile(outfile_facility_fac):
            os.remove(outfile_facility_fac)

        outfile_facility_country = self.db_dir+'/features/tmp_peeringdb/'+date.strftime("%Y-%m-%d")+"_facility_country.pkl"
        if os.path.isfile(outfile_facility_country):
            os.remove(outfile_facility_country)

        outfile_facility_cities = self.db_dir+'/features/tmp_peeringdb/'+date.strftime("%Y-%m-%d")+"_facility_cities.pkl"
        if os.path.isfile(outfile_facility_cities):
            os.remove(outfile_facility_cities)

        outfile_ixp = self.db_dir+'/features/tmp_peeringdb/'+date.strftime("%Y-%m-%d")+"_ixp.pkl"
        if os.path.isfile(outfile_ixp):
            os.remove(outfile_ixp)

def load_link_file(link_file):
    links = set()
    if os.path.exists(link_file):
        with open(link_file, "r") as f:
            for line in f:
                if '#' in line:
                    continue
                
                if "," in line:
                    as1 = int(line.split(",")[0].split(' ')[0])
                    as2 = int(line.split(",")[0].split(' ')[1])
                else:
                    as1 = int(line.split(' ')[0])
                    as2 = int(line.split(' ')[1])

                if int(as1) > int(as2):
                    as1, as2 = as2, as1

                # Add only the uniq links
                if (as1, as2) not in links:
                    links.add((as1, as2))
    # else:
    #     print("File {} has not been found in the local volume, skipped...".format(link_file), file=sys.stderr)

    return links



def get_all_dates(date_start, date_end):
    all_dates = []
    start_ts = mktime(datetime.strptime(date_start, "%Y-%m-%d").timetuple())
    end_ts = mktime(datetime.strptime(date_end, "%Y-%m-%d").timetuple())

    cur_ts = start_ts

        # while all the hours are not visited,...
    while cur_ts <= end_ts:
        start_tmp = datetime.utcfromtimestamp(cur_ts).strftime("%Y-%m-%d")
        all_dates.append(start_tmp)
            # ad a new process that will download all the events for the current houR
        cur_ts += 60 * 60 * 24

    return all_dates


def process_for_one_date(date, db_dir, method, feat, outfile=None):
    links = set()
    link_file = "{}/grip/asplist/{}T00:00_full.txt".format(db_dir, date)

    # Add all the links of the option --link_file
    if link_file is not None:
        links = load_link_file(link_file)

    if len(links) == 0:
        #print("No links found")
        return 1


    o = Orchestrator(method, feat, db_dir=db_dir)
    o.compute_nodes_features(date, True)
    o.compute_edge_features_links(date, outfile=outfile, links=links)
    o.clean_files(date)
    #exit(0)

'''
@click.command()
@click.option('--date', help='Date for which to compute peeringdb features, in the following format "YYYY-MM-DD".', type=str)
@click.option('--end_date', help='Date for which to compute peeringdb features, in the following format "YYYY-MM-DD".', type=str)
@click.option('--db_dir', default="db", help='Directory where is database.', type=str)
@click.option('--override', default=False, help='Override the existing output files. Default is False.', type=bool)
@click.option("--daily_sampling", default=False, help="Builds the daily sampling, in terms of positive and negative samples. Should be passed with option --date", type=bool)
@click.option("--link_list", default=None, help="List of links to test, in the form \"as1-as2,as3-as4,as5-as6\"", type=str)
@click.option("--link_file", default=None, help="file with the links to read. Each line of the file must be on the form \"as1 as2,whatever you want\" or \"as1 as2 whatever you want\". Basically, these files corresponds to the sampling files", type=str)
@click.option("--method", default="clusters", help="Sampling method used", type=str)
@click.option("--outfile", default=None, help="File to print the results", type=str)
'''




def launch_orchestrator(\
    date,\
    end_date='', \
    db_dir='db',\
    override=False,\
    daily_sampling=False,\
    link_list=None,\
    link_file=None,
    method="clusters",
    outfile=None, return_df=False,
    feat=["country_dist", "facility_fac_dist", "facility_country_dist", "facility_cities_dist", "ixp_dist"]):
    """Compute peeringDB features and store them in the database."""

    if daily_sampling:
        o = Orchestrator(method, feat, db_dir=db_dir)

        outfile_positive = db_dir+'/features/positive/peeringdb_{}/'.format(method)+date+"_positive.txt"
        outfile_negative = db_dir+'/features/negative/peeringdb/'+date+"_negative.txt"

        if (os.path.isfile(outfile_positive) and os.path.isfile(outfile_negative) and not override):
            print(o.print_prefix()+"Sampling for day {} already exists, skipped...".format(date), file=sys.stderr)
            return
        
        o.compute_nodes_features(date, override)
        o.compute_edge_features_daily_sampling(date, override)
        o.clean_files(date)

    elif end_date:
        dates = get_all_dates(date, end_date)
        proc_list = []
        # for d in dates:
        #     process_for_one_date(d, db_dir, method)
        with ProcessPoolExecutor(max_workers=1) as exec:
            exec.submit(process_for_one_date)
            for d in dates:
                proc_list.append(exec.submit(process_for_one_date, d, db_dir, method, feat))

            for p in proc_list:
                p.result()



    else:
        links = set()

        # Add all the links of the option --link_file
        if link_file is not None:
            links = load_link_file(link_file)

        # Add all the links of the option --link_list
        if link_list is not None:
            all_l = link_list.split(",")
            for l in all_l:
                as1 = int(l.split("-")[0])
                as2 = int(l.split("-")[1])

                if as1 > as2:
                    as1, as2 = as2, as1
                    
                link = (as1, as2)
                # Add only the uniq links
                if link not in links:
                    links.add(link)

        o = Orchestrator(method, feat=feat, db_dir=db_dir)
        o.compute_nodes_features(date, override)
        print("We compute node features for date {}".format(date), file=sys.stderr)
        if return_df:
            df = o.compute_edge_features_links(date, outfile=outfile, links=links, return_df=return_df)
            o.clean_files(date)
            print("We remove the files for date {}".format(date), file=sys.stderr)
            return df
        o.compute_edge_features_links(date, outfile, links=links)
        print("We Compute edge features for date {}".format(date), file=sys.stderr)
        o.clean_files(date)
        print("We remove the files for date {}".format(date), file=sys.stderr)

        #exit(0)


if __name__ == "__main__":
    launch_orchestrator()
