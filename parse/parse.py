from os import listdir
from os.path import isfile, join
from datetime import datetime

class Parser:
    def __init__(self, db_dir: str, date: str):
        self.db_dir = db_dir
        self.date = date

        # Load the details for every new edge case.
        tmp = date.split('-')
        date_p = '{}-{}-01'.format(tmp[0], tmp[1])
        infile_prefixes = '{}/prefixes/{}.txt'.format(db_dir, date_p)
        self.prefix_to_asns = {}
        with open(infile_prefixes, 'r') as fd:
            for line in fd.readlines():
                linetab = line.rstrip('\n').split(' ')
                prefix = linetab[0]
                
                if prefix not in self.prefix_to_asns:
                    self.prefix_to_asns[prefix] = set()
                
                for asn in linetab[1].split(','):
                    self.prefix_to_asns[prefix].add(asn)

                if len(linetab[1].split(',')) > 1:
                    print (line)

        self.dic_new_edges = {}
        self.dic_rec = {}
        self.new_edge_origin = {}
        with open('{}/new_edge/{}.txt'.format(self.db_dir, self.date)) as fd:
            for line in fd.readlines():
                if line.startswith('#'):
                    continue

                linetab = line.rstrip('\n').split(',')
                as1 = linetab[0].split(' ')[0]
                as2 = linetab[0].split(' ')[1]
                asp = linetab[1].split(' ')
                recurrent_case = linetab[3]

                # Make sure as1 is the lowest number.
                if int(as1) > int(as2):
                    as1, as2 = as2, as1

                if (as1, as2) not in self.dic_new_edges:
                    self.dic_new_edges[(as1, as2)] = set()
                    self.new_edge_origin[(as1, as2)] = set()

                if len(linetab) > 2:
                    info = linetab[2]
                    prefix = info.split('-')[1]
                    peerip = info.split('-')[2]
                    peerasn = info.split('-')[3]

                    self.dic_rec[(as1, as2)] = recurrent_case
                    self.dic_new_edges[(as1, as2)].add((peerip, peerasn))
                    self.new_edge_origin[(as1, as2)].add((asp[-1], prefix))


    def parse(self):
        infile = "{}/cases/{}.tmp".format(self.db_dir, self.date)
        outfile = "{}/cases/{}".format(self.db_dir, self.date)

        dic_res = {}
        dic_tags = {}
        fd_out = open(outfile, 'w', 1)

        with open(infile, 'r') as fd:
            for line in fd.readlines():
                if len(line.split(' '))<2:
                    continue
                linetab = line.rstrip('\n').split(' ')
                as1 = linetab[0]
                as2 = linetab[1]

                # Make sure as1 is the lowest number.
                try:
                    if int(as1) > int(as2):
                        as1, as2 = as2, as1
                except:
                    continue
                asp = linetab[2].split('|')
                label = int(linetab[3].strip(','))
                proba = linetab[4]
                sensitivity = linetab[5]

                if (as1, as2) not in dic_res:
                    dic_res[(as1, as2)] = {}
                if sensitivity not in dic_res[(as1, as2)]:
                    dic_res[(as1, as2)][sensitivity] = []
                dic_res[(as1, as2)][sensitivity].append(label)

                # Build the tags.
                if (as1, as2) not in dic_tags:
                    dic_tags[(as1, as2)] = {}

                # Tag Attacker/Victim.
                if 'attackers' not in dic_tags[(as1, as2)]:
                    dic_tags[(as1, as2)]['attackers'] = set()
                if 'victims' not in dic_tags[(as1, as2)]:
                    dic_tags[(as1, as2)]['victims'] = set()

                dic_tags[(as1, as2)]['victims'].add(asp[-1])

                if asp.index(as1) < asp.index(as2):
                    dic_tags[(as1, as2)]['attackers'].add(as1)
                else:
                    dic_tags[(as1, as2)]['attackers'].add(as2)

                # Type hijack type.
                hijack_type = len(asp)-min(asp.index(as1), asp.index(as2))-1
                if 'type' not in dic_tags[(as1, as2)]:
                    dic_tags[(as1, as2)]['type'] = set()
                dic_tags[(as1, as2)]['type'].add(hijack_type)

                # Tag local event.
                if 'peerasn' not in dic_tags[(as1, as2)]:
                    dic_tags[(as1, as2)]['peerasn'] = set()
                for peerinfo in self.dic_new_edges[(as1,as2)]:
                    dic_tags[(as1, as2)]['peerasn'].add(peerinfo[1])
            
                # Tag valid origin.
                valid_origin = True
                nb_processed = 0
                for origin, prefix in self.new_edge_origin[(as1, as2)]:
                    nb_processed += 1
                    if prefix in self.prefix_to_asns and origin not in self.prefix_to_asns[prefix]:
                        valid_origin = False
                        break

                    # Only check 1000 cases to avoid looping for too long.. (and 1000 should be largely enough).
                    if nb_processed == 1000:
                        break

                if 'valid_origin' not in dic_tags[(as1, as2)]:
                    dic_tags[(as1, as2)]['valid_origin'] = set([valid_origin])

                # Tag recurrence.
                if (as1, as2) in self.dic_rec:
                    dic_tags[(as1, as2)]['recurrent'] = [self.dic_rec[(as1, as2)]]
                else:
                    print ('Problem parsing {} {}'.format(as1, as2))

            # Finishing with tag local events.
            for (as1, as2) in dic_tags:
                dic_tags[(as1, as2)]['local'] = [False]
                # If there is only one attacker and only VPs in one AS that saw the event.
                if len(dic_tags[(as1, as2)]['attackers']) == 1 and \
                    len(dic_tags[(as1, as2)]['peerasn']) == 1:

                    # If the VP that sees the event is in the attacker AS.
                    if len(dic_tags[(as1, as2)]['peerasn'].intersection(dic_tags[(as1, as2)]['attackers'])) == 1:
                        dic_tags[(as1, as2)]['local'] = [True]
                        


        for as1, as2 in dic_res:
            sus = 0
            leg = 0
            asp_count = 0
            
            for sensitivity in dic_res[(as1, as2)]:
                if dic_res[(as1, as2)][sensitivity].count(0) > dic_res[(as1, as2)][sensitivity].count(1):
                    leg += 1
                else:
                    sus += 1
                asp_count = dic_res[(as1, as2)][sensitivity].count(0) + dic_res[(as1, as2)][sensitivity].count(1)
            
            # Write tags:
            s = ''
            for tags in dic_tags[(as1, as2)]:
                if tags == 'peerasn':
                    continue

                stmp = ''
                for e in dic_tags[(as1, as2)][tags]:
                    stmp += str(e)+','
                s += tags+':'+stmp[:-1]+';'

            if sus == 0:
                fd_out.write('!leg {} {} {} {} {} {}\n'.format(as1, as2, leg, sus, asp_count, s[:-1]))
            else:
                fd_out.write('!sus {} {} {} {} {} {}\n'.format(as1, as2, leg, sus, asp_count, s[:-1]))

            # Write inference result for every sensitivity.
            for sensitivity in dic_res[(as1, as2)]:
                

                fd_out.write("{} {} {} {} {}\n".format( \
                    as1, \
                    as2, \
                    sensitivity, \
                    dic_res[(as1, as2)][sensitivity].count(0), \
                    dic_res[(as1, as2)][sensitivity].count(1)))
                
        fd_out.close()

# def parse_dir(indir, outdir):
#     onlyfiles = [f for f in listdir(indir) if isfile(join(indir, f))]

#     for filename in onlyfiles:
#         parse('{}/{}'.format(indir, filename), '{}/{}'.format(outdir, filename))        


def launch_parser(\
    db_dir, \
    date):
    """
    This script parses the outfile provided by the broker.
    In case a directory is given as input, it parsed all the files in the directory.
    """

    p = Parser(db_dir, date)
    p.parse()
