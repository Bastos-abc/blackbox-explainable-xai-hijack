import os
import requests
import bgpkit
from concurrent import futures
import networkx as nx
from datetime import datetime, timedelta

from utils.mvp import get_vps
from utils.vps import get_vps_info
from utils.cleaning import remove_asprepending

from colorama import Fore
from colorama import Style
from colorama import init
init(autoreset=True)

class CollectUpdates:
    def __init__(self, nb_vps: int=10, max_workers: int=4):

        # Max number of processes when download mrt files.
        self.max_workers = max_workers

        # Get list of vantage points with MVP.
        self.nb_vps = nb_vps
        self.vps_info = get_vps_info()
        vps_set = get_vps(nb_vps)

        # Retrieve a list of collectors from which to download data
        self.collectors = set()
        for vp in vps_set:
            self.collectors.add(vp[0])
        
        # From the VP's asn, retrieve the corresponding peer's IPs.
        self.peers = []

        for collector, asn in vps_set:
            if asn in self.vps_info:
                for vp in self.vps_info[asn]:
                    if vp[0].lower() == collector.lower():
                        self.peers.append(vp[2])

    def print_prefix(self):
        return Fore.YELLOW+Style.BRIGHT+"[collect_updates.py]: "+Style.NORMAL

    def update_peers(self, nb_vps):
        self.vps_set = get_vps(nb_vps)

        # Retrieve a list of collectors from which to download data
        self.collectors = set()
        for vp in self.vps_set:
            self.collectors.add(vp[0])
        
        # From the VP's asn, retrieve the corresponding peer's IPs.
        self.peers = []

        for collector, asn in self.vps_set:
            if asn in self.vps_info:
                for vp in self.vps_info[asn]:
                    if vp[0].lower() == collector.lower():
                        self.peers.append(vp[2])

    # Function to load the ixps number from ixp file.
    def get_ixps(self, infile):
        ixps = set()

        if os.path.isfile(infile):
            with open(infile, 'r') as fd:
                for line in fd.readlines():
                    ixps.add(line.rstrip('\n'))

        return ixps

    # Helper function that uses the bgpkit broker to retrieve MRT files to
    # download.
    def query_helper(params_list):
        broker = bgpkit.Broker()
        return broker.query( \
            ts_start=params_list[0], \
            ts_end=params_list[1], \
            data_type=params_list[2], \
            collector_id=params_list[3])

    # Helper function that uses the bgpkit parser to download and parse the
    # MRT files.
    def parser_helper(params_list):
        # print ('start: ', params_list[0], params_list[1])
        parser = bgpkit.Parser( \
            url=params_list[0], \
            filters={"peer_ips": params_list[1], \
                'type':'announce'})

        failed = True
        while failed:
            try:
                tmp = parser.parse_all()
                failed = False
            except:
                print (self.print_prefix()+'BGPKIT parser failed with params {}'.format(parser))

        # print ('stop: ', params_list[0], len(tmp))
        return tmp

    def build_snapshot(self, ts_start: str=None, ts_end: str=None, ixp_file: str=None, outfile: str=None):
        # AS-level topology build from the AS paths.
        topo = nx.DiGraph()

        # Load IXP ASN file.
        ixp_set = self.get_ixps(ixp_file)

        # Initialize the current start and end date.
        cur_ts_start = ts_start
        cur_end_start = ts_start + timedelta(hours=1)

        # Process the data hour by hour to limit memory utilization.
        while cur_end_start <= ts_end:
            cur_nb_vps = self.nb_vps
            failed = True

            while failed:
                self.update_peers(cur_nb_vps)
                print (self.print_prefix()+'{} -> {}: Searching for BGP MRT file (nb_vps={})'.format(cur_ts_start, cur_end_start, len(self.peers)))
                
                try:
                    # Get the MRT file download.
                    params_query = []
                    for c in self.collectors:
                        params_query.append((cur_ts_start, cur_end_start, 'update', c)) 

                    parser_params = []
                    with futures.ProcessPoolExecutor(self.max_workers) as executor:
                        for result in executor.map(CollectUpdates.query_helper, params_query):
                            for broker_item in result:
                                parser_params.append([broker_item.url, ', '.join(self.peers)])

                    print (self.print_prefix()+'{} -> {}: BGP MRT files found, starting to parse them...'.format(cur_ts_start, cur_end_start))

                    with futures.ProcessPoolExecutor(self.max_workers) as executor:
                        for result in executor.map(CollectUpdates.parser_helper, parser_params):
                            for bgp_route in result:
                                # Transform the as path into a list of integers.
                                try:
                                    aspath = list(map(lambda x: int(x), bgp_route['as_path'].split(' ')))
                                except ValueError:
                                    continue
                                # Clean up the as path.
                                aspath = remove_asprepending(aspath, ixp_set)

                                if aspath is not None:
                                    # Update the topology.
                                    for i in range(0, len(aspath)-1):
                                        topo.add_edge(aspath[i], aspath[i+1])
                                else:
                                    print (bgp_route)
                                    
                    failed = False
                except:
                    failed = True
                    # Divide the number of VPs to use by two.
                    cur_nb_vps = cur_nb_vps/2.
                    self.update_peers(cur_nb_vps)

                    print (self.print_prefix()+'Execution has failed, retrying ...')

            # Move to the next hour.
            cur_ts_start = cur_end_start
            cur_end_start = cur_end_start + timedelta(hours=1)

        print (self.print_prefix()+'{} -> {}: topo size: {} {}'.format(ts_start, ts_end, topo.number_of_nodes(), topo.number_of_edges()))
        
        # Print in a file the resulting topology.
        if outfile is not None:
            with open(outfile, 'w') as fd:
                for as1, as2 in topo.edges():
                    fd.write("{} {}\n".format(as1, as2))

if __name__ == "__main__":
    cu = CollectUpdates(nb_vps=20, max_workers=20)
    cu.build_snapshot(ts_start="2022-05-20T00:00:00", ts_end="2022-05-21T00:00:00", outfile="topo.txt")