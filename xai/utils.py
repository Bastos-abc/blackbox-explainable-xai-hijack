from sklearn.model_selection import GridSearchCV
from sklearn.ensemble import RandomForestClassifier
import pickle
import bz2
import inference.utils as ut
import inference.build_dataset as bds
from sklearn.model_selection import train_test_split
import os


def forest_GridSearchCV(X_train, Y_train, fp_weight):
    clf = RandomForestClassifier()

    param_grid = {
        # 'criterion': ['gini', 'entropy'],
        'n_estimators': [100],
        'max_features': ['log2'],
        'max_depth': [12],
        'min_weight_fraction_leaf': [0.],
        'min_samples_split': [2]
    }

    grid_search = GridSearchCV(estimator=clf, param_grid=param_grid, cv=2, n_jobs=1)
    grid_search.fit(X_train, Y_train.values.ravel())

    return grid_search


def forest_build(X_train, Y_train, date, db_dir, features, write=True, fp_weight=1):
    X_train, X_grid, Y_train, Y_grid = train_test_split(X_train, Y_train, train_size=0.9)

    model_file = "{}/models/{}_model_{}_{}.pkl".format(db_dir, date, ",".join(sorted(features)), fp_weight)
    if os.path.isfile(model_file):
        clf = load_model(model_file)
    else:
        clf = RandomForestClassifier(
            max_depth=12, \
            min_weight_fraction_leaf=0., \
            min_samples_split=2,
            max_features='log2',
            n_estimators=100,
            class_weight={0: fp_weight, 1: 1})

        # print("Fitting with a dataset of size {}".format(len(X_train.index)))
        clf.fit(X_train, Y_train.values.ravel())

        if len(features) == 4 or (len(features) == 3 and "aspath" not in features) and write:
            with bz2.BZ2File("{}/models/{}_model_{}_{}.pkl".format(db_dir, date, ",".join(sorted(features)), fp_weight),
                             "wb") as f:
                pickle.dump(clf, f)

    return clf


def build_model_for_day(db_dir, date, features, fp_weight, method, nb_days=60):
    X = bds.build_training_set(date, db_dir, features, method, nb_days=nb_days)
    if X is None or len(X.index) == 0:
        ut.err_msg("Unable to load any data, exit...")
        return None, None, None

    Y = X["label"]
    X = X.drop(columns=["as1", "as2", "label"])

    # Build the random forest
    clf = forest_build(X, Y, date, db_dir, features, fp_weight=fp_weight)
    return Y, X, clf


def load_model(fn):
    with bz2.BZ2File(fn, "rb") as f:
        clf = pickle.load(f)

    return clf