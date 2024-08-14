import os
import pandas as pd
from time import time
import shlex
import random
import string
import subprocess
import runner.utils as ut



def copy_file_to_container(fn, db_dir, id):
    #cmd = shlex.split("cp {} {}/tmp/{}_{}".format(fn, db_dir, os.path.basename(fn), id))
    cmd = "cp {} {}/tmp/{}_{}".format(fn, db_dir, os.path.basename(fn), id)
    print(cmd)
    #subprocess.run(cmd)
    os.system(cmd)
    return "{}/tmp/{}_{}".format(db_dir, os.path.basename(fn), id)

def remove_file_in_container(fn, docker_name=''):
    #cmd = shlex.split("docker exec -it {} rm {}".format(docker_name, fn))
    #print (cmd)
    print('Removing:', fn)
    #os.remove(fn)
    #subprocess.run(cmd)


def run_aspath_features(asplist, date, db_dir, aspath_feat, fn=None, id=0):
    import aspath_feat as aspf
    list_aspath = None
    fn_tmp = None
    if fn is None:
        aspf.run_orchestrator(date="{}".format(date), db_dir=db_dir)
        list_aspath = ""

        for (as1, as2, asp) in asplist:
            list_aspath += "{} {},{}-".format(as1, as2, asp)
        if len(list_aspath):
            list_aspath = list_aspath.rstrip(list_aspath[-1])
        else:
            return None

    else:
        # Copy the file in the docker container.
        fn_tmp = copy_file_to_container(fn, db_dir, id)


    df = aspf.run_orchestrator(date="{}".format(date), db_dir=db_dir, aspath_list=list_aspath, aspath_file=fn_tmp,
                               return_df=True, metrics=aspath_feat)

    #if fn is not None:
    #    os.remove(fn_tmp)

    if df is None:
        return None

    return df


def run_topological_features(asplist, date, db_dir, feat_exclude, fn=None, id=0):
    import topo_feat
    list_aspath = None
    fn_tmp = None
    if fn is None:
        list_aspath = ""

        all_links = []
        for (as1, as2, asp) in asplist:
            if (as1, as2) not in all_links:
                list_aspath += "{}-{},".format(as1, as2)
                all_links.append((as1, as2))

        if len(list_aspath):
            list_aspath = list_aspath.rstrip(list_aspath[-1])
        else:
            return None


    else:
        # Copy the file in the docker container.
        fn_tmp = copy_file_to_container(fn, db_dir, id)

    df = topo_feat.run_orchestrator(nb_threads=10, date=date, db_dir=db_dir, link_list=list_aspath,
                                    link_file=fn_tmp, return_df=True, feat_exclude=feat_exclude)

    if fn is not None:
        os.remove(fn_tmp)

    # Stop the container.
    #stop_container(docker_name)

    if df is None:
        return None

    return df


def run_bidir_features(asplist, date, db_dir, bidi_feat, fn=None, id=0):
    import bidirectionality as bidi
    list_aspath = None
    fn_tmp = None
    if fn is None:
        list_aspath = ""

        all_links = []
        for (as1, as2, asp) in asplist:
            if (as1, as2) not in all_links:
                list_aspath += "{}-{},".format(as1, as2)
                all_links.append((as1, as2))

        if len(list_aspath):
            list_aspath = list_aspath.rstrip(list_aspath[-1])
        else:
            return None
        
    else:
        # Copy the file in the docker container.
        fn_tmp = copy_file_to_container(fn, db_dir, id)

    df = bidi.launch_orchestrator(db_dir=db_dir, date="{}".format(date), link_list=list_aspath, link_file=fn_tmp,
                                  return_df=True, feat=bidi_feat)

    if fn is not None:
        # Remove the input file from the container.
        remove_file_in_container(fn_tmp)


    if df is None:
        return None

    return df


def run_peeringdb_features(asplist, date, db_dir, peer_feat, fn=None, id=0):
    import peering
    list_aspath = None
    fn_tmp = None

    if fn is None:
        list_aspath = ""

        all_links = []
        for (as1, as2, asp) in asplist:
            if (as1, as2) not in all_links:
                list_aspath += "{}-{},".format(as1, as2)
                all_links.append((as1, as2))

        if len(list_aspath):
            list_aspath = list_aspath.rstrip(list_aspath[-1])
        else:
            return None


    else:
        # Copy the file in the docker container.
        fn_tmp = copy_file_to_container(fn, db_dir, id)



    df = peering.launch_orchestrator(date="{}".format(date), db_dir=db_dir, link_list=list_aspath, link_file=fn_tmp,
                                     return_df=True, feat=peer_feat)

    if fn is not None:
        # Remove the input file from the container.
        remove_file_in_container(fn_tmp)

    # Stop the container.
    #stop_container(docker_name)

    if df is None:
        return None

    return df


def run_features(asplist, date, db_dir, aspath_feat, bidi_feat, peer_feat, topo_feat_exclude, fn=None, id=0):
    df = dict()

    start = time()
    df["bidirectionality"] = run_bidir_features(asplist, date, db_dir, bidi_feat, fn=fn, id=id)
    print("Bidirectionality feature took {:.2f} seconds".format(time() - start))
    

    start = time()
    df["peeringdb"] = run_peeringdb_features(asplist, date, db_dir, peer_feat, fn=fn, id=id)
    print("Peeringdb feature took {:.2f} seconds".format(time() - start))
    

    start = time()
    df["topological"] = run_topological_features(asplist, date, db_dir, topo_feat_exclude, fn=fn, id=id)
    print("Topological feature took {:.2f} seconds".format(time() - start))
    
    start = time()
    df["aspath"] = run_aspath_features(asplist, date, db_dir, aspath_feat, fn=fn, id=id)
    print("ASpath feature took {:.2f} seconds".format(time() - start))

    feat_available = []

    for feat in df.keys():
        if df[feat] is not None:
            feat_available.append(feat)
            #with open("tmp_values_{}.txt".format(feat), "w") as f:
            #    f.write(df[feat].to_csv(index=False, sep=" "))

    if len(feat_available) == 0:
        ut.err_msg("No feature type are available, abort...")
        return None, None


    feat_ref = feat_available[0]


    X = df[feat_ref]

    #print(X.keys())

    for feat in feat_available[1:]:
        #print(feat)
        #print(df[feat])
        X = X.merge(df[feat], how="inner", on=["as1", "as2"])
        #print(X.keys())
    '''
    in_file = []
    if os.path.isfile(fn):
        lines = open(fn, 'r')
        for line in lines:
            if line.startswith('#'):
                continue
            tmp = line.split(',')
            #Teste
            asp=tmp[1].replace(' ','|').replace('\n','')
            #asp = list(map(lambda x:int(x), tmp[1].split(' ')))
            as1 = int(tmp[0].split(' ')[0])
            as2 = int(tmp[0].split(' ')[1])
            if as1 > as2:
                t = as1
                as1 = as2
                as2 = t
            in_file.append([as1, as2, asp])
    
    tmp_df = pd.DataFrame(in_file, columns=['as1','as2', 'asp'])
    X = X.merge(tmp_df, how="inner", on=["as1", "as2"])
    '''
    return X, feat_available


def run_inference(in_df, db_dir, feats, date, nb_days_training_data, id, outfile=None):
    import inference_maker as infm
    with open(db_dir+"/tmp/inference_{}.txt".format(id), "w") as f:
        f.write(in_df.to_csv(index=False, sep=" "))

    print('Running inference')
    #fpr_weights ="1,5,10"
    fpr_weights = "1,2,3,4,5,6,7,8,9,10"
    df = infm.run_inference_maker(date="{}".format(date), input_file="{}/tmp/inference_{}.txt".format(db_dir,id),
                                  fpr_weights=fpr_weights,  overide=0, nb_days_training_data=nb_days_training_data,
                                  db_dir=db_dir, features="{}".format(",".join(feats)), return_df=False,
                                  outfile=outfile)
    #print('DataFrame size:', len(df))
    os.remove(db_dir+"/tmp/inference_{}.txt".format(id))

    if df is None or not outfile is None:
        return None

    return df



if __name__ == "__main__":
    date = "2022-01-02"
    asplist = []
    asplist.append(("12389", "15497", "395152 14007 3356 20764 12389 15497"))
    asplist.append(("12389", "50673", "20634 8447 20764 12389 50673"))
    asplist.append(("12389", "50673", "8676 3356 174 20764 12389 50673"))
    asplist.append(("12389", "20764", "8676 3356 174 20764 12389 50673"))

    df, feats = run_features(asplist, date)
    res = run_inference(df, feats, date)

    print(res)



