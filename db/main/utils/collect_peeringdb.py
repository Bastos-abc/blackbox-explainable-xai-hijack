import wget
import urllib
from datetime import datetime

from colorama import Fore
from colorama import Style
from colorama import init
init(autoreset=True)

def print_prefix():
    return Fore.BLUE+Style.BRIGHT+"[collect_peeringdb.py]: "+Style.NORMAL


def collect_peeringdb(ts, outfile, outfile_caidaixp):
    datem = datetime.strptime(ts, "%Y-%m-%d")

    url = "https://publicdata.caida.org/datasets/peeringdb/{}/{}/peeringdb_2_dump_{}_{}_{}.json".format(datem.year, datem.strftime('%m'), datem.year, datem.strftime('%m'), datem.strftime('%d'))
    print (print_prefix()+"Download peeringDB file: {}".format(url))

    datem.replace(day=1)
    month_nb = int(datem.month)

    if month_nb == 2 or month_nb == 3:
        month_nb = 1
    elif month_nb == 5 or month_nb == 6:
        month_nb = 4
    elif month_nb == 8 or month_nb == 9:
        month_nb = 7
    elif month_nb == 11 or month_nb == 12:
        month_nb = 10

    datem.replace(month=month_nb)
    url_caida_ixp = "https://publicdata.caida.org/datasets/ixps/ix-asns_{}{}.jsonl".format(datem.year, '%02d' % month_nb)
    print (print_prefix()+"Download CAIDA IXP file: {}".format(url_caida_ixp))

    try:
        wget.download(url, outfile, bar=None)
        wget.download(url_caida_ixp, outfile_caidaixp, bar=None)
        return True
    except urllib.error.HTTPError:
        print (print_prefix()+'PeeringDB or CAIDA IXP file not found')
        return False
    

if __name__ == "__main__":
    collect_peeringdb("2019-01-01")