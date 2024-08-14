import pandas as pd
from colorama import Fore, Style
import os
import json
from datetime import datetime
#from time import mktime
import sys
import random

#import runner.utils as ut
import run_features as rf


def print_prefix(msg, end="\n"):
    currentTime = datetime.now().strftime("%H:%M:%S")
    s = Fore.GREEN + Style.BRIGHT + "[broker.py ({})]: ".format(currentTime) + Style.NORMAL + msg + Fore.WHITE
    print(s, end=end, file=sys.stderr)


class RequestBroker:
    def __init__(self, date, db_dir, aspath_feat, bidi_feat, peer_feat, topo_feat_exclude, nb_days_training):
        self.date = date
        self.db_dir = db_dir
        self.results = []
        self.aspath_feat = aspath_feat
        self.bidi_feat = bidi_feat
        self.peer_feat = peer_feat
        self.topo_feat_exclude = topo_feat_exclude
        self.nb_days_training = nb_days_training

    def process_request(self, infile, idn=None, outfile=None):
        # Generate a random number used to uniqly identify the temporary files.
        # print('inside process_request')
        cases_feat_folder = '{}/cases_features/'.format(self.db_dir)
        if not os.path.isdir(cases_feat_folder):
            os.mkdir(cases_feat_folder)
        if not idn:
            idn = random.randint(0, 1000000)
        feat_files = '{}/{}.csv'.format(cases_feat_folder, self.date)
        if os.path.isfile(feat_files):
            df = pd.read_csv(feat_files, sep=' ')
            feats = ["bidirectionality", "peeringdb", "topological", "aspath"]
        else:
            df, feats = rf.run_features(None, self.date, self.db_dir, self.aspath_feat, self.bidi_feat, self.peer_feat,
                                        self.topo_feat_exclude, fn=infile, id=idn)
            df.to_csv('{}/{}.csv'.format(cases_feat_folder, self.date), index=False, sep=" ")
        res = rf.run_inference(df, self.db_dir, feats, self.date, self.nb_days_training, idn, outfile)
        print('[broker.py] Finishing inference :')#,len(res.index))
        # print(res)
        if not res is None:

            for i in range(0, len(res.index)):
                #j += 1
                #print('Broker line 42:', j, 'of', len(res.index))
                line = dict()
                for feat in res.keys():
                    line[feat] = str(res[feat].values[i])
                self.results.append(line.copy())
        del df
        # print(self.results)


    def to_json(self):
        return json.dumps(self.results)

    def to_text(self):
        s = ''
        for l in self.results:
            s += l['as1'] + ' '
            s += l['as2'] + ' '
            if 'asp' in l:
                s += l['asp'] + ' '
            else:
                s += 'None' + ' '
            s += l['label'] + ' '
            s += l['proba'] + ' '
            s += l['sensitivity'] + '\n'

        return s[:-1]

    def clear(self):
        self.results.clear()
        self.results = []







