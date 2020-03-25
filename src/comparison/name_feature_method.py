import warnings

import numpy as np
import pandas as pd
from sklearn.model_selection import GroupShuffleSplit
from sklearn.utils import shuffle

from comparison.feature_group import inner_name_features, outer_name_features
from eutilities.customized_print import pprint
from eutilities.preprocessor import scale
from help.dataset_split import check_group_split
from metric.metric import calc_metrics, metric_names
from model.available_model import ModelName
from model.classification import use_classifier

warnings.filterwarnings('ignore')

mode_names = ModelName.available_modes()
print('available_modes: ', mode_names)
meta_columns = ['pm_ao1', 'pm_ao2', 'same_author', 'source', 'shared_lastname', 'lastname_hash_partition_for_split']


def run(method='innername_features', dataset='GS', cached_file_path='../cached/pubmed_inner_outer_feature.tsv'):
    df = pd.read_csv(cached_file_path, sep='\t')
    df = df[df['source'] == dataset]
    print(df.shape)
    feature_names = inner_name_features if method == 'innername_features' else outer_name_features
    df = df[meta_columns + feature_names]
    print('original shape: ', df.shape)
    df = shuffle(df)

    # prepare X
    X = df[feature_names]
    X = scale(X)
    X = np.array(X)
    print(X.shape)
    # prepare Y
    Y = df['same_author'].astype('int').values
    shared_lastname = df['shared_lastname'].values
    lastname_hash = df['lastname_hash_partition_for_split'].values
    kf = GroupShuffleSplit(n_splits=10)
    indx_split = kf.split(X, groups=lastname_hash)
    check_group_split(shared_lastname, indx_split)

    for idx, model_switch in enumerate(mode_names):
        X_copy, Y_copy = X.copy(), Y.copy()
        print('-' * 160)
        avg_metrics = []
        indx_split = kf.split(X, groups=lastname_hash)
        for train_index, test_index in indx_split:
            train_X, train_y = X_copy[train_index], Y_copy[train_index]
            test_X, test_y = X_copy[test_index], Y_copy[test_index]
            pred_y, feature_importance = use_classifier(train_X, train_y, test_X, model_switch=model_switch)
            metric_dict = calc_metrics(test_y, pred_y)
            avg_metrics.append(metric_dict)
        avg_metric_vals = [np.average([item[m] for item in avg_metrics]) for m in metric_names]
        print()
        print('model: ', model_switch, 'method: ', method, 'dataset: ', dataset)
        print(metric_names)
        pprint(list(zip(metric_names, avg_metric_vals)), pctg=True, sep='\t')


if __name__ == '__main__':
    run(method='innername_features', dataset='GS')
    run(method='outername_features', dataset='GS')

    run(method='innername_features', dataset='SONG')
    run(method='outername_features', dataset='SONG')
