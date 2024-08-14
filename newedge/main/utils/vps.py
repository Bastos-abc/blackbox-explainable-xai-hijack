import os
import re
import urllib.request
from datetime import datetime

def get_routeviews_peers():
    url = 'https://www.routeviews.org/peers/peering-status.html'

    peer_list = []
    for line in urllib.request.urlopen(url).read().decode().split('\n'):
        if 'routeviews.org' in line:
            line = ' '.join(line.split())
            meta = line.split('|')[0]
            metatab = meta.split(' ')

            collector = metatab[0].replace('.routeviews.org', '')
            asn = int(metatab[1])
            peer_addr = metatab[2]
            nb_pref = int(metatab[3])

            peer_list.append((collector, peer_addr, asn, nb_pref))

    return peer_list

def get_ris_peers():
    url = 'https://www.ris.ripe.net/peerlist/all.shtml'

    peer_list = []
    cur_collector = None

    cur_line = 0
    for line in urllib.request.urlopen(url).read().decode().split('\n'):
        line = line.strip()

        if '<h2> RRC' in line:
            cur_collector = line.split(' -- ')[0].replace('<h2>', '').strip()

        # Get the AS number.
        if cur_line == 0 and line.startswith('<td> <a href="https://stat.ripe.net/'):
            linetab = line.split('<td>')
            asn = int(linetab[1].split('>')[1].replace('</a', '').replace('AS', ''))
            cur_line += 1

        elif cur_line == 1: 
            linetab = line.split('<td>')
            name = linetab[1].replace('</td>', '').strip()
            cur_line += 1
        
        elif cur_line == 2:
            linetab = line.split('<td>')
            peerip = linetab[1].replace('</td>', '').strip()
            cur_line += 1

        elif cur_line == 3:
            linetab = line.split('<td>')
            nb_pref_ipv4 = int(linetab[1].replace('</td>', '').strip())
            cur_line += 1

        elif cur_line == 4:
            linetab = line.split('<td>')
            nb_pref_ipv6 = int(linetab[1].replace('</td>', '').strip())
            cur_line = 0

            peer_list.append((cur_collector, peerip, asn, nb_pref_ipv4+nb_pref_ipv6))

    return peer_list    

def get_vps_info():
    peers_list_rv = get_routeviews_peers()
    peers_list_ris = get_ris_peers()

    dic_peer = {} # ASN -> vps.
    for collector, peerip, asn, nb_pref in peers_list_rv+peers_list_ris:
        if asn not in dic_peer:
            dic_peer[asn] = set()
        dic_peer[asn].add((collector, asn, peerip))

    return dic_peer

if __name__ == "__main__":
    peer_list = get_ris_peers()
    print (peer_list)
