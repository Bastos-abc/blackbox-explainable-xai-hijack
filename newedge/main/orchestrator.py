import os
import csv
import networkx as nx
from datetime import datetime, timedelta
import click 

from colorama import Fore
from colorama import Style
from colorama import init
init(autoreset=True)

from utils.get_paths import GetPath


class NewEdgeFinder:
    def __init__(self, db_dir: str, nb_vps: int, max_workers: int):
        self.db_dir = db_dir
        self.nb_vps = nb_vps
        self.max_workers = max_workers
        self.prefix_dir = 'new_edge'

        # Init the database directory for the new edge cases if not created yet.
        if not os.path.isdir(self.db_dir+'/'+self.prefix_dir):
            os.mkdir(self.db_dir+'/'+self.prefix_dir)

    def print_prefix():
        return Fore.WHITE+Style.BRIGHT+"[NewEdgeFinder]: "+Style.NORMAL

    def get_ixp_filename(self, date):
        # Find the ixp list file correponding to the date.
        dateixp = date.replace(day=1)
        month_nb = dateixp.month

        if month_nb == 2 or month_nb == 3:
            month_nb = 1
        elif month_nb == 5 or month_nb == 6:
            month_nb = 4
        elif month_nb == 8 or month_nb == 9:
            month_nb = 7
        elif month_nb == 11 or month_nb == 12:
            month_nb = 10

        dateixp.replace(month=month_nb)
        return self.db_dir+'/peeringdb/'+dateixp.strftime("%Y-%m-%d")+"_ixplist.txt"

    # Helper function to iterate between two dates.
    def daterange(start_date, end_date):
        for n in range(int((end_date - start_date).days)):
            yield start_date + timedelta(n)

    def compute_new_edge(self, datestr: str, nbdays: int):
                        
        date = datetime.strptime(datestr, "%Y-%m-%dT%H:%M:%S")

        # Get the date for the first day of the X previous month.
        first_day = date - timedelta(days=nbdays)
        out_filename = self.db_dir + '/' + self.prefix_dir + '/' + date.strftime("%Y-%m-%d") + ".txt"
        if os.path.isfile(out_filename):
            print('All done!!!!!!!!!!!!!!!!')
            print('File already exist', out_filename)
            return None
        # All the suspicious cases detected the last nbdays days (to omit them).
        suspicious_edges = {}
        for cur_date in NewEdgeFinder.daterange(first_day, date):
            case_filename = self.db_dir+'/cases/'+cur_date.strftime("%Y-%m-%d")
            if os.path.isfile(case_filename):
                with open(case_filename, 'r') as fd:
                    for line in fd.readlines():
                        if line.startswith('!sus'):
                            linetab = line.rstrip().split(' ')
                            as1 = int(linetab[1])
                            as2 = int(linetab[2])

                            if (as1, as2) not in suspicious_edges:
                                suspicious_edges[(as1, as2)] = cur_date

                        if line.startswith('!leg'):
                            linetab = line.rstrip().split(' ')
                            as1 = int(linetab[1])
                            as2 = int(linetab[2])
                            if (as1, as2) in suspicious_edges:
                                del suspicious_edges[(as1, as2)]

        print (NewEdgeFinder.print_prefix()+"Number of suspicious edges: {}.".format(len(suspicious_edges)))

        # Load the graph before the date.
        topo_before = nx.Graph()

        filename = self.db_dir+'/merged_topology/'+(date - timedelta(days=1)).strftime("%Y-%m-%d")+".txt"
        if os.path.isfile(filename): 
            with open(filename, 'r') as fd:
                csv_reader = csv.reader(fd, delimiter=' ')
                for row in csv_reader:
                    as1 = int(row[0])
                    as2 = int(row[1])
                    if ((as1, as2) not in suspicious_edges and (as2, as1) not in suspicious_edges) \
                        or ((as1, as2) in suspicious_edges and (date-suspicious_edges[(as1, as2)]).days > 31) \
                        or ((as2, as1) in suspicious_edges and (date-suspicious_edges[(as2, as1)]).days > 31):
                        topo_before.add_edge(as1, as2)
                    else:
                        if (as1, as2) in suspicious_edges:
                            print ('{} {} not added because suspicious {}'.format(as1, as2, suspicious_edges[(as1, as2)]))
                        else:
                            print ('{} {} not added because suspicious {}'.format(as1, as2, suspicious_edges[(as2, as1)]))

        # Check the diff with the edges in the current day to find the new edges.
        filename = self.db_dir+'/topology/'+date.strftime("%Y-%m-%d")+"_updates.txt"
        topo_after = nx.Graph()
        if os.path.isfile(filename): 
            with open(filename, 'r') as fd:
                csv_reader = csv.reader(fd, delimiter=' ')
                for row in csv_reader:
                    # Search for new link
                    # Either the new link did not exist.
                    if not topo_before.has_edge(int(row[0]), int(row[1])):
                        topo_after.add_edge(int(row[0]), int(row[1]))

        # Get the ixp list filename.
        month_first_day = datetime.strptime(datestr, "%Y-%m-%dT%H:%M:%S").replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        ixp_file = self.get_ixp_filename(month_first_day)
        if not os.path.isfile(ixp_file): 
            print (self.print_prefix()+"IXP File {} not available, continuing without it.".format(ixp_file))

        print (NewEdgeFinder.print_prefix()+datestr+': New edges computed. Found {} new edges.'.format(topo_after.number_of_edges()))

        gp = GetPath(self.nb_vps, self.max_workers)
        edge_paths = gp.collect_paths(ts_start=date, ts_end=date + timedelta(days=1), edges=set(topo_after.edges()), ixp_file=ixp_file)

        # Print the new edge cases
        filename = self.db_dir+'/'+self.prefix_dir+'/'+date.strftime("%Y-%m-%d")+".txt"
        with open(filename, 'w', 1) as fd:
            fd.write('# Number of edges found: {}\n'.format(topo_after.number_of_edges()))
            for as1, as2 in edge_paths:
                if (int(as1), int(as2)) in suspicious_edges or (int(as2), int(as1)) in suspicious_edges:
                    past_sus = True
                else:
                    past_sus = False

                for aspath in edge_paths[(as1, as2)]:
                    
                    # Retrieve all the timestamp, peer_ip and peer_asn
                    str_tmp = ''
                    timestamp, prefix, peer_ip, peer_asn = edge_paths[(as1, as2)][aspath]
                    str_tmp += "{}-{}-{}-{},".format(int(timestamp), prefix, peer_ip, peer_asn)
                    str_tmp = str_tmp[:-1]

                    fd.write("{} {},{},{},{}\n".format(as1, as2, aspath, str_tmp, past_sus))


# Make the CLI.
@click.command()
@click.option('--date', help='Date for which to collect the full topology, in the following format "YYYY-MM-DDThh:mm:ss".', type=str)
@click.option('--nb_vps', default=10, help='Number of vantage points from which to download updates data .', type=int)
@click.option('--max_workers', default=4, help='Maximum number of workers when downloading the updates.', type=int)
@click.option('--db_dir', default="db", help='Directory where is database.', type=str)

def compute_new_edge(\
    date, \
    nb_vps, \
    max_workers, \
    db_dir):
    """ Get the new edge links that appear in a given day.
    This script relies on the merged topology.
    If they are not in the database, it builds them first."""

    nef = NewEdgeFinder( \
        db_dir=db_dir, \
        nb_vps=nb_vps, \
        max_workers=max_workers)
    nef.compute_new_edge(date, 300)

if __name__ == "__main__":
    compute_new_edge()
