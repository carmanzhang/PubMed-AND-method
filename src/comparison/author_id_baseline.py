import warnings

import pandas as pd
from sklearn.utils import shuffle

from eutilities.customized_print import pprint
from metric.metric import calc_metrics, metric_names
from model.available_model import ModelName
from src.io.data_reader import DBReader

warnings.filterwarnings('ignore')

mode_names = ModelName.available_modes()
print('available_modes: ', mode_names)

meta_columns = ['pm_ao1', 'pm_ao2', 'same_author', 'source', 'shared_lastname', 'lastname_hash_partition_for_split']


def run(which_external_id='same_mag_author_id', dataset='GS',
        cached_file_path='../cached/SONG_GS_WHU_mix_dataset_external_author_id.pkl'):
    df = DBReader.cached_read(cached_file_path=cached_file_path,
                              sql="select * from and.SONG_GS_WHU_mix_dataset_external_author_id;", cached=True)
    columns = df.columns.values
    print(len(columns), columns)
    df = df[df['source'] == dataset]
    df = shuffle(df)
    print(df.shape)
    df = df[df[which_external_id] != -1]
    same_author = df['same_author'].values
    is_same_author_id = df[which_external_id].values
    metrics = calc_metrics(same_author, is_same_author_id)
    metrics = [metrics[n] for n in metric_names]
    print('which_external_id: ', which_external_id, 'dataset: ', dataset)
    pprint(list(zip(metric_names, metrics)), pctg=True, sep='\t')
    print('-' * 50)


if __name__ == '__main__':
    run(which_external_id='same_mag_author_id', dataset='GS')
    run(which_external_id='same_s2_author_id', dataset='GS')
    run(which_external_id='same_pkg_aid_v2_author_id', dataset='GS')

    run(which_external_id='same_mag_author_id', dataset='SONG')
    run(which_external_id='same_s2_author_id', dataset='SONG')
    run(which_external_id='same_pkg_aid_v2_author_id', dataset='SONG')
