import warnings

import numpy as np
import pandas as pd
from sklearn.model_selection import GroupShuffleSplit
from sklearn.utils import shuffle

from comparison.feature_group import outername_inner_outermags2pkg_features, outer_name_features, \
    inner_features, outer_mag_s2_pkg_features
from eutilities.customized_print import pprint
from eutilities.preprocessor import scale
from metric.metric import calc_metrics, metric_names
from model.available_model import ModelName
from model.classification import use_classifier

warnings.filterwarnings('ignore')

mode_names = ModelName.available_modes()
print('available_modes: ', mode_names)
meta_columns = ['pm_ao1', 'pm_ao2', 'same_author', 'source', 'shared_lastname', 'lastname_hash_partition_for_split']

feature_group = {
    'outer_name_features': outer_name_features,
    'inner_features': inner_features,
    'outer_mag_s2_pkg_features': outer_mag_s2_pkg_features,
    'exclude_outer_name_features': list(set(inner_features + outer_mag_s2_pkg_features)),
    'exclude_inner_features': list(set(outer_name_features + outer_mag_s2_pkg_features)),
    'exclude_outer_mag_s2_pkg_features': list(set(inner_features + outer_name_features)),
    'outername_inner_outermags2pkg_features': outername_inner_outermags2pkg_features
}


def run(method='outername_inner_features', dataset='GS',
        cached_file_path='../cached/pubmed_inner_outer_feature.tsv'):
    df = pd.read_csv(cached_file_path, sep='\t')
    df = df[df['source'] == dataset]
    print(df.shape)
    feature_names = feature_group[method]
    df = df[meta_columns + feature_names]
    print('original shape: ', df.shape)
    df = shuffle(df)

    for idx, model_switch in enumerate(mode_names):
        df_copy = df.copy(deep=True)
        Y = np.array(df_copy['same_author'].astype('int'))
        X = df_copy[feature_names]
        X = scale(X)
        X = np.array(X)

        avg_metrics = []
        # kf = KFold(n_splits=10, shuffle=True)
        # indx_split = kf.split(Y)
        kf = GroupShuffleSplit(n_splits=10)
        indx_split = kf.split(X, groups=df['lastname_hash_partition_for_split'].values)
        for train_index, test_index in indx_split:
            train_X, train_y = X[train_index], Y[train_index]
            test_X, test_y = X[test_index], Y[test_index]
            pred_y, feature_importance = use_classifier(train_X, train_y, test_X, model_switch=model_switch)
            metric_dict = calc_metrics(test_y, pred_y)
            avg_metrics.append(metric_dict)

        avg_metric_vals = [np.average([item[m] for item in avg_metrics]) for m in metric_names]
        print('model: ', model_switch, 'method: ', method, 'dataset: ', dataset)
        pprint(list(zip(metric_names, avg_metric_vals)), pctg=True, sep='\t')
        print('-' * 160)


if __name__ == '__main__':
    run(method='outer_name_features', dataset='GS')
    run(method='inner_features', dataset='GS')
    run(method='outer_mag_s2_pkg_features', dataset='GS')
    run(method='exclude_outer_name_features', dataset='GS')
    run(method='exclude_inner_features', dataset='GS')
    run(method='exclude_outer_mag_s2_pkg_features', dataset='GS')
    run(method='outername_inner_outermags2pkg_features', dataset='GS')

    run(method='outer_name_features', dataset='SONG')
    run(method='inner_features', dataset='SONG')
    run(method='outer_mag_s2_pkg_features', dataset='SONG')
    run(method='exclude_outer_name_features', dataset='SONG')
    run(method='exclude_inner_features', dataset='SONG')
    run(method='exclude_outer_mag_s2_pkg_features', dataset='SONG')
    run(method='outername_inner_outermags2pkg_features', dataset='SONG')
