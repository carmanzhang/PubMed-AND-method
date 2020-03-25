import time
import warnings

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import GroupShuffleSplit
from sklearn.utils import shuffle

from eutilities.preprocessor import scale
from eutilities.string_utils import jaccard_similarity, intersection
from metric.metric import calc_metrics

warnings.filterwarnings('ignore')

meta_columns = ['pm_ao1', 'pm_ao2', 'same_author', 'source', 'shared_lastname', 'lastname_hash_partition_for_split']


def randomforest_regressor(X_train, Y_train, X_test):
    model = RandomForestRegressor(n_estimators=100,
                                  max_depth=None,
                                  min_samples_split=2,
                                  min_samples_leaf=1,
                                  max_features='auto',  # "auto" class_weight='balanced'
                                  )
    model.fit(X_train, Y_train)
    y_pred = model.predict(X_test)
    return y_pred, model.feature_importances_


def run(feature_names, dataset='GS',
        cached_file_path='cached/pubmed_inner_outer_feature.tsv'):
    print(feature_names)
    df = pd.read_csv(cached_file_path, sep='\t')
    df = df[df['source'] == dataset]
    print(df.shape)
    df = df[meta_columns + feature_names]
    print('original shape: ', df.shape)
    df = shuffle(df)

    df_copy = df.copy(deep=True)
    Y = np.array(df_copy['same_author'].astype('int'))
    X = df_copy[feature_names]

    pm_ao1_arr, pm_ao2_arr = df_copy['pm_ao1'].values, df_copy['pm_ao2'].values
    X = df_copy[feature_names]

    X = scale(X)
    X = np.array(X)

    kf = GroupShuffleSplit(n_splits=5)
    indx_split = kf.split(X, groups=df['lastname_hash_partition_for_split'].values)
    for i, (train_index, test_index) in enumerate(indx_split):
        if i > 0:
            break
        train_X, train_y = X[train_index], Y[train_index]
        test_X, test_y = X[test_index], Y[test_index]
        test_pm_ao1, test_pm_ao2 = pm_ao1_arr[test_index], pm_ao2_arr[test_index]
        pred_y, feature_importance = randomforest_regressor(train_X, train_y, test_X)
        print(sorted(zip(feature_names, feature_importance), key=lambda x: x[1], reverse=True))
        print('\t'.join([str(n) for n in feature_importance]))
        metric_dict = calc_metrics(test_y, pred_y)
        # print(metric_dict)
        time_stamp = int(time.time())
        # assert len(test_pm_ao1) == len(test_pm_ao2) == len(test_y) == len(pred_y)
        # v = tcp_client.execute(query="insert into and.AggAND_test_set_result_for_error_analysis VALUES",
        #                        params=[list(test_pm_ao1), list(test_pm_ao2), list(test_y), list(pred_y), [time_stamp] * len(pred_y)],
        #                        columnar=True)
        # print(v)


if __name__ == '__main__':
    # run and get error cases, feature contributions
    # run(feature_names=outername_inner_outermags2pkg_features, dataset='GS')

    # run and get feature values from raw data
    # outer_name_based_features_1 -> full name jaccard
    # journal_based_features_1 -> Journal descriptors

    author_info = [
        [('yanina', 'dubrovskaya'), ('yanina', 'dubrovskaya'), ['communicable diseases', 'microbiology', 'hospitals'],
         ['communicable diseases', 'drug therapy', 'anti-bacterial agents']],
        [('guang-ying', 'huang'), ('guang-ying', 'huang'), ['reproductive medicine', 'gynecology', 'obstetrics'],
         ['complementary therapies', 'psychology', 'psychiatry']],
        # [('raul', 'ortiz de lejarazu'), ('ra�l', 'ortiz de lejarazu'),
        #  ['virology', 'communicable diseases', 'pediatrics'],
        #  ['virology', 'acquired immunodeficiency syndrome', 'communicable diseases']],
        [('shuuichirou', 'asaumi'), ('sunao', 'asaumi'), ['traumatology', 'nephrology', 'endocrinology'],
         ['nephrology', 'transplantation', 'urology']],
        # [('jing', 'deng'), ('jing', 'deng'), ['allergy and immunology', 'cell biology', 'biochemistry'],
        #  ['cardiology', 'vascular diseases', 'cell biology']],
        # [('jack h', 'bloch'), ('jack h', 'bloch'), ['orthopedics', 'vascular diseases', 'microbiology'],
        #  ['transplantation', 'nephrology', 'nutritional sciences']],
        # [('annamaria', 'pellecchia'), ('andrew', 'pellecchia'), ['neoplasms', 'urology', 'cell biology'],
        #  ['communicable diseases', 'microbiology', 'diagnostic imaging']],
        [('jana k', 'geuer'), ('jana k', 'geuer'), ['psychopharmacology', 'drug therapy', 'psychiatry'],
         ['environmental health', 'toxicology', 'microbiology']],
        # [('yoko', 'kaneto'), ('yoko', 'kaneto'), ['cardiology', 'vascular diseases', 'pulmonary medicine'],
        #  ['toxicology', 'pharmacology', 'gastroenterology']],
        # [('huseyin', 'karaaslan'), ('huseyin', 'karaaslan'), ['nephrology', 'endocrinology', 'transplantation'],
        #  ['rheumatology', 'orthopedics', 'chemistry techniques. analytical']],
        # [('f', 'domenech'), ('f', 'domenech'), ['anatomy', 'pathology', 'allergy and immunology'],
        #  ['biotechnology', 'environmental health', 'biomedical engineering']]
    ]

    for (firstname1, lastname1), (firstname2, lastname2), jd1, jd2 in author_info:
        name_sim = jaccard_similarity(list(lastname1 + firstname1), list(lastname2 + firstname2))
        jd_sim = intersection(jd1, jd2)  # GS JD
        print(name_sim, jd_sim)
