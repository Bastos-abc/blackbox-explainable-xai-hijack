import os
import pickle
import csv
import pandas as pd
import networkx as nx
from datetime import datetime, timedelta

from colorama import Fore
from colorama import Style
from colorama import init
init(autoreset=True)


def print_prefix():
    return Fore.BLUE+Style.BRIGHT+"[bidirectionality.py]: "+Style.NORMAL

def topo_merger_bgp_irr(bgp_topo_file, irr_topo_file):
    topo = nx.DiGraph()

    # Read the topology inferred from BGP updates and rib.
    with open(bgp_topo_file, 'r') as fd:
        for line in fd.readlines():
            if line.startswith('#'):
                continue

            linetab = line.rstrip('\n').split(' ')
            as1 = int(linetab[0])
            as2 = int(linetab[1])

            topo.add_edge(as1, as2)

    # Read the topology inferred from IRR.
    with open(irr_topo_file, 'r') as fd:
        for line in fd.readlines():
            if line.startswith('#'):
                continue

            linetab = line.rstrip('\n').split(' ')
            try:
                as1 = int(linetab[0].replace('as', ''))
                as2 = int(linetab[1])
            except ValueError:
                pass
                # print (print_prefix()+'Error in the IRR parsed graph: {} in {}'.format(linetab, irr_topo_file)) 

            topo.add_edge(as1, as2)

    return topo


def bidirectional_links(links, rib_file: str, bgp_topo_files: list, irr_topo_files: list, threshold_days_appearance: int=2 ):
    df = pd.DataFrame(columns=['as1', 'as2', 'bidi'])
    topo_updates = nx.DiGraph()
    topo_rib = nx.DiGraph()

    # First, Merge topology (irr & bgp together) from multiple consecutive days.
    for bgp_topo_file, irr_topo_file in zip(bgp_topo_files, irr_topo_files):
        print(print_prefix()+'Processing update files {} and {}.'.format(bgp_topo_file, irr_topo_file))

        tmp = topo_merger_bgp_irr(bgp_topo_file, irr_topo_file)

        # For every edge, count the number of days that it appears.
        for as1, as2 in tmp.edges():
            if topo_updates.has_edge(as1, as2):
                topo_updates[as1][as2]['count'] += 1
            else:
                topo_updates.add_edge(as1, as2)
                topo_updates[as1][as2]['count'] = 1

    # Second, load the rib dump of the next month, if it exists.
    print(print_prefix()+'Processing RIB file {}.'.format(rib_file))

    if rib_file is not None:
        with open(rib_file, 'r') as fd:
            for line in fd.readlines():
                if line.startswith('#'):
                    continue

                linetab = line.rstrip('\n').split(' ')
                as1 = int(linetab[0])
                as2 = int(linetab[1])

                topo_rib.add_edge(as1, as2)

    for as1, as2 in links:
        if (topo_updates.has_edge(as1, as2) and \
            topo_updates.has_edge(as2, as1) and \
            topo_updates[as1][as2]['count'] >= threshold_days_appearance and \
            topo_updates[as2][as1]['count'] >= threshold_days_appearance) or\
            topo_rib.has_edge(as1, as2) and topo_rib.has_edge(as2, as1):

            df.loc[len(df)] = [as1, as2, 1]
        else:
            df.loc[len(df)] = [as1, as2, 0]

    return df
