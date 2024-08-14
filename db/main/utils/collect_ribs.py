import os
import bz2
import requests
import networkx as nx
import bgpkit
from datetime import datetime, timedelta
from concurrent import futures

from colorama import Fore
from colorama import Style
from colorama import init
init(autoreset=True)

from utils.cleaning import remove_asprepending
from utils.mvp import get_vps
from utils.vps import get_vps_info

class CollectRibs:
    def __init__(self, nb_vps: int=20, max_workers: int=10):

        # Max number of processes when download mrt files.
        self.max_workers = max_workers

        # Get list of vantage points with MVP.
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
        return Fore.CYAN+Style.BRIGHT+"[collect_ribs.py]: "+Style.NORMAL

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

        return tmp

    def build_snapshot(self, date: str=None, ixp_file:str=None, outfile: str=None, outfile_paths: str=None, outfile_prefixes: str=None):
        date = datetime.strptime(date, "%Y-%m-%d")

        # Initialize the current start and end date. We download the RIB files at midnight.
        date_start = date + timedelta(minutes=-10)
        end_start = date + timedelta(minutes=+10)

        # AS-level topology build from the AS paths.
        topo = nx.DiGraph()
        allpaths = set()
        asn_prefix_mapping = {}

        # Load IXP ASN file.
        ixp_set = self.get_ixps(ixp_file)

        print (self.print_prefix()+'{} -> {}: Searching for BGP MRT file (nb_vps={})'.format(date_start.strftime("%Y-%m-%d"), end_start.strftime("%Y-%m-%d"), len(self.peers)))
        # Get the MRT file download.
        params_query = []
        for c in self.collectors:
            params_query.append((date_start, end_start, 'rib', c)) 

        parser_params = []
        with futures.ProcessPoolExecutor(self.max_workers) as executor:
            for result in executor.map(CollectRibs.query_helper, params_query):
                for broker_item in result:
                    parser_params.append([broker_item.url, ', '.join(self.peers)]) 

        print (self.print_prefix()+'{} -> {}: BGP MRT files found, starting to parse them...'.format(date_start.strftime("%Y-%m-%d"), end_start.strftime("%Y-%m-%d")))

        failed = 0
        while failed < 3:
            try:
                with futures.ProcessPoolExecutor(self.max_workers) as executor:
                    for result in executor.map(CollectRibs.parser_helper, parser_params):
                        for bgp_route in result:
                            # Transform the as path into a list of integers.
                            try:
                                aspath = list(map(lambda x: int(x), bgp_route['as_path'].split(' ')))
                            except ValueError:
                                continue
                            # Clean up the as path.
                            aspath = remove_asprepending(aspath, ixp_set)
                            allpaths.add(' '.join(list(map(lambda x:str(x), aspath))))

                            if aspath is not None:
                                # Update the ASN to prefix mapping.
                                if aspath[-1] not in asn_prefix_mapping:
                                    asn_prefix_mapping[aspath[-1]] = set()
                                asn_prefix_mapping[aspath[-1]].add(bgp_route['prefix'])

                                # Update the topology.
                                for i in range(0, len(aspath)-1):
                                    topo.add_edge(aspath[i], aspath[i+1])
                            else:
                                print (bgp_route)
                failed = 3
            except:
                failed += 1
                if failed < 3:
                    print (self.print_prefix()+'Execution has failed, retrying ...')
                else:   
                    print (self.print_prefix()+'Execution has failed again, moving to the next hour ...')

        # Print in a file the resulting topology.
        if outfile is not None:
            with open(outfile, 'w') as fd:
                for as1, as2 in topo.edges():
                    fd.write("{} {}\n".format(as1, as2))

        # Print in a file the resulting unique as path.
        if outfile_paths is not None:
            with open(outfile_paths, 'w') as fd:
                for path in allpaths:
                    fd.write(path+'\n')

        # Print in a file the resulting asn to prefix mapping.
        if outfile_prefixes is not None:
            with open(outfile_prefixes, 'w') as fd:
                for asn in asn_prefix_mapping:
                    for p in asn_prefix_mapping[asn]:
                        fd.write('{} {}\n'.format(p, asn))

        print (self.print_prefix()+'{} -> {}: BGP MRT files processed successfully'.format(date_start.strftime("%Y-%m-%d"), end_start.strftime("%Y-%m-%d")))


    def build_snapshot_caida(self, date: str=None, ixp_file: str=None, outfile: str=None, outfile_paths: str=None):
        # AS-level topology build from the AS paths.
        topo = nx.DiGraph()

        # Set with all the AS paths seen.
        allpaths = set()

        # Load IXP ASN file.
        ixp_set = self.get_ixps(ixp_file)

        print (self.print_prefix()+'{}: Start to extract the all-path file.'.format(date))

        # Extracting the bz2 from CAIDA dataset is done is a streaming manner, to avoid saving the entire extracted file.
        decompressor = bz2.BZ2Decompressor()
        prev_data = b''
        chunk_size = 10024
        leftover_str = ''
        total_size_read = 0

        # Download the as path file from CAIDA's website.
        with requests.get("https://publicdata.caida.org/datasets/as-relationships/serial-1/{}.all-paths.bz2".format(date)) as res:
            print (self.print_prefix()+'{}: RIB file downloaded.'.format(date))
            # Decompress every chunk of the file.
            itern = 0
            for chunk in res.iter_content(chunk_size=10024):
                total_size_read += len(chunk)
                dc = decompressor.decompress(chunk)
                
                lines = (leftover_str+dc.decode("utf-8")).split('\n')
                for line in lines[:-1]:
                    aspath = list(map(lambda x:int(x), line.split(' ')[1].split('|')))
                    aspath = remove_asprepending(aspath, ixp_set)
                    allpaths.add(' '.join(list(map(lambda x:str(x), aspath))))

                    # Update the topology.
                    for i in range(0, len(aspath)-1):
                        topo.add_edge(aspath[i], aspath[i+1])

                # In case the chunk stop at the middle of a line.
                leftover_str = lines[-1]

                # Specific to bz2 decompression, see doc.
                if decompressor.eof == True:
                    leftover = decompressor.unused_data
                    decompressor = bz2.BZ2Decompressor()

                    dc = decompressor.decompress(leftover)
                    
                    lines = (leftover_str+dc.decode('utf-8')).split('\n')
                    for line in lines[:-1]:
                        aspath = list(map(lambda x:int(x), line.split(' ')[1].split('|')))
                        aspath = remove_asprepending(aspath)

                        # Update the topology.
                        for i in range(0, len(aspath)-1):
                            topo.add_edge(aspath[i], aspath[i+1])
                    
                    # In case the chunk stop at the middle of a line.
                    leftover_str = lines[-1]

                itern += 1
                if itern%1000 == 0:
                    print (self.print_prefix()+'{}: topo size: {} {} size: {}Mb'.format(date, topo.number_of_nodes(), topo.number_of_edges(), total_size_read/1000000.))

        # Print in a file the resulting topology.
        if outfile is not None:
            with open(outfile, 'w') as fd:
                for as1, as2 in topo.edges():
                    fd.write("{} {}\n".format(as1, as2))

        # Print in a file the resulting unique as path.
        if outfile_paths is not None:
            with open(outfile_paths, 'w') as fd:
                for path in allpaths:
                    fd.write(path+'\n')

if __name__ == "__main__":
    cr = CollectRibs(max_workers=1)
    cr.build_snapshot('20040201', 'tmp.txt')
