from __future__ import unicode_literals, print_function

import string

import unicodedata
from pandahouse import read_clickhouse, to_clickhouse

from client.clickhouse_client import CH

# Turn a Unicode string to plain ASCII, thanks to https://stackoverflow.com/a/518232/2809427
all_letters = string.ascii_letters + " -"
print(all_letters)
all_letters = set([c for c in all_letters])
n_letters = len(all_letters)


def unicodeToAscii(s):
    s = s.lower()
    return ''.join(
        c for c in unicodedata.normalize('NFD', s)
        if unicodedata.category(c) != 'Mn'
    )


conn = CH(host='localhost', http_port='8124').get_conn()
# max_row_num = 119781963
# num_portions = 10
# row_num_starts = [int(max_row_num * 1.0 * i / num_portions) for i in range(num_portions)]
# row_num_ends = [i - 1 for i in row_num_starts[1:]] + [max_row_num - 1]
# print(row_num_starts)
# print(row_num_ends)

sql_template = """
select pm_ao,
       one_author[2]                               as last_name,
       one_author[3]                               as first_name,
       one_author[4]                               as initials,
       replaceAll(lowerUTF8(mag_author_name), '.', '') as mag_author_name,
       replaceAll(lowerUTF8(s2_author_name), '.', '')  as s2_author_name
from and.pm_aminer_s2_mag_paper_mapping where toUInt32OrZero(substring(pm_ao, 2,1))=%d;"""

for s in range(0, 10):
    sql = sql_template % s
    print(sql)
    df = read_clickhouse(sql, index=False,
                         connection=conn, encoding='utf-8', stream=True)

    print('load dataframe')
    shape = df.shape
    print('df shape: ', shape)

    df['last_name'] = df['last_name'].apply(unicodeToAscii)
    print('done last_name transformation')
    df['first_name'] = df['first_name'].apply(unicodeToAscii)
    print('done first_name transformation')
    df['mag_author_name'] = df['mag_author_name'].apply(unicodeToAscii)
    print('done mag_author_name transformation')
    df['s2_author_name'] = df['s2_author_name'].apply(unicodeToAscii)
    print('done s2_author_name transformation')
    to_clickhouse(df, 'pm_s2_mag_author_name_normalization', chunksize=1000000, index=False, connection=conn)
