import click
from multiprocessing import Pool
from trustee import ClassificationTrustee
from sklearn.metrics import classification_report
from sklearn import tree
import graphviz
import os
import pdfplumber
from xai.utils import build_model_for_day


def exec_trustee(outfolder, clf, y, X, train=[]):
    folder = '{}/Analysis_{}_{}_{}'.format(outfolder, train[0], train[1], train[2])
    if not (os.path.isdir(folder)):
        os.mkdir(folder)
    log_file = folder + '/feature_analyses_all_trustee.log'

    trustee = ClassificationTrustee(expert=clf)
    log = open(log_file, 'w')
    print(f'trustee.fit(X, y, num_iter={train[0]}, num_stability_iter={train[1]}, samples_size={train[2]}, verbose=False)')
    print(f'trustee.fit(X, y, num_iter={train[0]}, num_stability_iter={train[1]}, samples_size={train[2]}, verbose=False)', file=log)
    print(folder, file=log)
    trustee.fit(X, y, num_iter=train[0], num_stability_iter=train[1], samples_size=train[2], verbose=False)

    # Get the best explanation from Trustee
    dt, pruned_dt, agreement, reward = trustee.explain()
    print(f"Model explanation training (agreement, fidelity): ({agreement}, {reward})", file=log)
    print(f"Model Explanation size: {dt.tree_.node_count}", file=log)
    print(f"Top-k (k=10)Prunned Model explanation size: {pruned_dt.tree_.node_count}", file=log)

    # Use explanations to make predictions
    dt_y_pred = dt.predict(X)
    pruned_dt_y_pred = pruned_dt.predict(X)
    y_pred = clf.predict(X)
    # Evaluate accuracy and fidelity of explanations
    print("Model explanation global fidelity report:", file=log)
    print(classification_report(y_pred, dt_y_pred), file=log)
    print("Top-k Model explanation global fidelity report:", file=log)
    print(classification_report(y_pred, pruned_dt_y_pred), file=log)

    # Output decision tree to pdf
    dot_data = tree.export_graphviz(
        dt,
        class_names=["Legitimate", "Hijacker"],
        # feature_names=features,
        feature_names=X.columns,
        filled=True,
        rounded=True,
        special_characters=True,
    )
    graph = graphviz.Source(dot_data)
    graph.render('{}/dt_explanation_C_{}_{}_{}'.format(folder, train[0], train[1], train[2]))

    # Output pruned decision tree to pdf
    dot_data = tree.export_graphviz(
        pruned_dt,
        class_names=["Legitimate", "Hijacker"],
        # feature_names=features,
        feature_names=X.columns,
        filled=True,
        rounded=True,
        special_characters=True,
    )
    graph = graphviz.Source(dot_data)
    graph.render('{}/pruned_dt_explation_C_{}_{}_{}'.format(folder, train[0], train[1], train[2]))
    print(X.columns, file=log)
    print(trustee.get_top_features(top_k=10), file=log)
    print('----End----', file=log)
    log.close()


def get_files(root, f_start):
    folders = [root]
    files = []
    while len(folders) > 0:
        folder = folders.pop()
        tmp = os.listdir(folder)
        for t in tmp:
            atual = folder+'/'+t
            if os.path.isdir(atual):
                if t.startswith('Analise'):
                    folders.append(atual)
            elif os.path.isfile(atual):
                if t.startswith(f_start) and t.endswith('.pdf'):
                    files.append(atual)

    return files


def features_analyse(files, features):
    analysed = dict()
    for feature in features:
        analysed[feature] = {'dt': 0, 'total': 0, 'root': 0}
    for file in files:
        pdf = pdfplumber.open(file)
        page = pdf.pages[0]
        text = page.extract_text()
        lines = text.split('\n')
        i = 0
        f_arvore=[]
        for line in lines:
            words = line.split(' ')
            for word in words:
                if word in features:
                    analysed[word]['total'] += 1
                    if not word in f_arvore:
                        f_arvore.append(word)
                        analysed[word]['dt'] += 1
                    if i == 0:
                        analysed[word]['root'] += 1
            i += 1

    return analysed

def exec_analyse(folder, analise, features):
    print('Analisando arquivos existentes!!!!')
    files = get_files(folder, analise)
    print(folder, analise)
    print('Coletando informações dos PDF!!!!')
    print('Total de arquivos:', len(files))
    analysed = features_analyse(files, features)
    output = open('{}/features_{}.csv'.format(folder, analise), 'w')
    print('Feature,DT,Total,root', file=output)
    for feature in analysed.keys():
        print(feature, analysed[feature]['dt'], analysed[feature]['total'], analysed[feature]['root'], sep=',',
              file=output)
    # print(text)
    output.close()


@click.command()
@click.option('--date', help='Date for which to compute peeringdb features, in the following format "YYYY-MM-DD".', type=str)
@click.option('--db_dir', help='Database directory".', type=str)
@click.option('--n_threads', help='Number of threads".', type=int)
@click.option('--outfolder', help='Folder to save the files".', type=str)
def launch_test(date, db_dir, n_threads, outfolder):
    if not os.path.isdir(outfolder):
        os.mkdir(outfolder)

    features = "aspath,bidirectionality,peeringdb,topological"
    features = sorted(features.split(","))
    fp_weight = '1'
    method = "clusters"
    Y, X, clf = build_model_for_day(db_dir, date, features, fp_weight, method, nb_days=60)
    if Y is None:
        print('Data not loaded!!!!')
        exit(1)
    # Trustee parameters
    num_inter = [40, 50]
    num_stability_inter = [5, 10]
    samples_size = [0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]
    train_trustee = []
    for ni in num_inter:
        for nsi in num_stability_inter:
            for ss in samples_size:
                train_trustee.append([ni, nsi, ss])

    args = []
    for train in train_trustee:
        folder = '{}/Analysis_{}_{}_{}'.format(outfolder, train[0], train[1], train[2])
        if os.path.isdir(folder):
            log_file = folder + '/feature_analyses_all_trustee.log'
            if os.path.isfile(log_file):
                test = open(log_file, 'r')
                last_line = ''
                for line in test:
                    last_line = line
                test.close()
                if '----End----' in last_line:
                    continue
        args.append([outfolder, clf, Y, X, train])

    with Pool(processes=n_threads) as th_pool:
        th_pool.starmap(exec_trustee, args,)

    analises = ['dt', 'pruned']
    features = X.columns
    for analise in analises:
        exec_analyse(outfolder, analise, features)


if __name__ == '__main__':
    launch_test()
