import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler, StandardScaler


def drop_missing_items(df):
    df = df.dropna(how='any')
    return df


def down_sample(df, percent=1):
    '''
    percent:多数类别下采样的数量相对于少数类别样本数量的比例
    '''
    data0 = df[df['same_author'] == 0]  # 将多数类别的样本放在data0
    data1 = df[df['same_author'] == 1]  # 将少数类别的样本放在data1
    index = np.random.randint(
        len(data0), size=percent * (len(df) - len(data0)))  # 随机给定下采样取出样本的序号
    lower_data1 = data0.iloc[list(index)]  # 下采样
    # print(lower_data1.shape)
    # print(data1.shape)
    return (pd.concat([lower_data1, data1]))


def scale(df):
    mm_scaler = MinMaxScaler()
    df = mm_scaler.fit_transform(df)
    std_scaler = StandardScaler()
    df = std_scaler.fit_transform(df)
    return df
