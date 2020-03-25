-- 准备4个数据集，按照以下步骤：
-- 1. 导入外部数据集到数据库中
-- 2. 匹配pubmed文章，根据pmid+姓名的缩写，匹配pmid+author order
-- 3. 将数据集转换成 author pair 的形式

--数据集1：SONG -------------------------------------------------------------------------------------------------------------------------------------------------
-- cat and_corpus.txt | dos2unix | clickhouse-client --password root --port 9001 --query='insert into and.SONG FORMAT TSVWithNames'
create table if not exists and.SONG
(
    IDX                    String,
    PMID                   String,
    First_Author_Last_Name String,
    First_Author_Initials  String
) ENGINE = Log;

-- we finally construct the AND training set, which contains 385 authors and 2875 publications.
-- In other words, the final AND training set, a gold standard author set,
-- has 385 authors among 36 groups of authors, and 2875 publications.

-- 2875	2875	36	41	385
select count()                                                                                     as cnt,
       count(distinct PMID)                                                                        as PMIDs,
       count(distinct First_Author_Last_Name)                                                      as last_names,
       count(distinct concat(First_Author_Last_Name, '_', substring(First_Author_Initials, 1, 1))) as namesapces,
       count(distinct IDX)                                                                         as IDXs
from and.SONG;

-- 28925
select sum(pairs) as all_pairs
from (
      select IDX, count() as cnt, cnt * (cnt - 1) / 2 as pairs from and.SONG group by IDX);


create table if not exists and.SONG_author_pair_generated_by_external_script
(
    pm_id_1     String,
    pm_id_2     String,
    same_author String
) ENGINE = Log;


-- 组合IDX
-- drop table and.SONG_author_pair;
create materialized view if not exists and.SONG_author_pair ENGINE = Log populate as
select *
from (
         select *
         from (
                  select namespace,
                         substring(arrayElement(res, 1) as pm_idx_1, 1, position(pm_idx_1, '_') - 1) as pm_id_1,
                         substring(arrayElement(res, 2) as pm_idx_2, 1, position(pm_idx_2, '_') - 1) as pm_id_2,
                         substring(pm_idx_1, position(pm_idx_1, '_') + 1)                            as idx_1,
                         substring(pm_idx_2, position(pm_idx_2, '_') + 1)                            as idx_2,
                         if(idx_1 = idx_2, 1, 0)                                                     as same_author
                  from (
                           select groupArray(concat(PMID, '_', IDX)) as pm_idxs,
--                                   concat(First_Author_Last_Name, '_', substring(First_Author_Initials, 1, 1)) as namespace
                                  First_Author_Last_Name             as namespace -- 他们论文中说了有36个group，他们使用的是lastname作为group划分的标志
                           from and.SONG
                           group by namespace)
                           array join
                       arrayMap(x -> splitByChar(':', x), arrayFilter(
                               x -> substring(x, 1, position(x, ':') - 1) !=
                                    substring(x, position(x, ':') + 1, length(x)),
                               arrayDistinct(arrayFlatten(arrayMap(x -> arrayMap(
                                       y -> if(x > y,
                                               concat(toString(y), ':', toString(x)),
                                               concat(toString(x), ':', toString(y))), pm_idxs), pm_idxs))))) as res)
                  any
                  left join (select IDX                    as IDX1,
                                    PMID                   as pm_id_1,
                                    First_Author_Last_Name as First_Author_Last_Name1,
                                    First_Author_Initials  as First_Author_Initials1
                             from and.SONG) using pm_id_1) any
         left join (select IDX                    as IDX2,
                           PMID                   as pm_id_2,
                           First_Author_Last_Name as First_Author_Last_Name2,
                           First_Author_Initials  as First_Author_Initials2
                    from and.SONG) using pm_id_2;

-- 0	154765
-- 1	28925
select same_author, count()
from and.SONG_author_pair
group by same_author;

-- 验证了CH对 author pair 生成的正确性
-- 0	154765
-- 1	28925
select same_author, count()
from and.SONG_author_pair_generated_by_external_script
group by same_author;

-- 验证了CH对 author pair 生成的正确性
select count()
from (
         select toString(same_author)                                 as buildin_calculated_same_author,
                arrayStringConcat(arraySort([pm_id_1, pm_id_2]), '|') as connector
         from and.SONG_author_pair) any
         inner join (select *,
                            same_author                                           as external_calculated_same_author,
                            arrayStringConcat(arraySort([pm_id_1, pm_id_2]), '|') as connector
                     from and.SONG_author_pair_generated_by_external_script)
                    using connector
where buildin_calculated_same_author != external_calculated_same_author;

select First_Author_Last_Name, IDX, groupUniqArray(First_Author_Initials), count() as cnt
from and.SONG
group by First_Author_Last_Name, IDX;

-- 183690
select count()
from and.SONG_author_pair;

-- 0	154765
-- 1	28925
select same_author, count()
from and.SONG_author_pair
group by same_author;

-- drop table and.SONG_dataset;
create materialized view and.SONG_dataset ENGINE = Log populate as
select pm_id1,
       lastname1,
       initials1,
       pm_id2,
       lastname2,
       initials2,
       same_author,
       rand() % 10 <= 6 ? 1 : 0 as is_train, -- left out 30% data for testing
       author_list1,
       author_list2,
--        arrayElement(arrayFilter(x -> length(x) > 0,
--                                 arrayMap(x->if(lower(x[2]) = lastname1 and lower(x[4]) = initials1, x[1], ''),
--                                          author_list1)), 1) as matched_author_order1,
       '1'                      as matched_author_order1,
--        arrayElement(arrayFilter(x -> length(x) > 0,
--                                 arrayMap(x->if(lower(x[2]) = lastname2 and lower(x[4]) = initials2, x[1], ''),
--                                          author_list2)), 1) as matched_author_order2,
       '1'                      as matched_author_order2
from (
         select pm_id1,
                lastname1,
                initials1,
                pm_id2,
                lastname2,
                initials2,
                same_author,
                author_list1
         from (
                  select pm_id_1                        as pm_id1,
                         lower(First_Author_Last_Name1) as lastname1,
                         lower(First_Author_Initials1)  as initials1,
                         pm_id_2                        as pm_id2,
                         lower(First_Author_Last_Name2) as lastname2,
                         lower(First_Author_Initials2)  as initials2,
                         same_author
                  from and.SONG_author_pair) as t1 any
                  left join (select pm_id, author_list as author_list1 from and.nft_paper_author_name_list) as t2
                            on t1.pm_id1 = t2.pm_id) as t3 any
         left join (select pm_id, author_list as author_list2 from and.nft_paper_author_name_list) as t4
                   on t3.pm_id2 = t4.pm_id;

-- 183690
select count()
from and.SONG_dataset;

-- SONG使用的都是第一作者，所以这里先不篇匹配作者的顺序
-- 如何通过lastname+initials匹配作者，则SONG_dataset的大小是 183218。 通过surname + initials的方式匹配到了183218个，剩余的400多个没有匹配到
select count()
from and.SONG_dataset
where length(matched_author_order1) > 0
  and length(matched_author_order2) > 0;


--数据集2：GS -------------------------------------------------------------------------------------------------------------------------------------------------
-- clickhouse-client --password root --port 9001 --format_csv_delimiter=';' --query='insert into and.GS_train FORMAT CSVWithNames' < ./1500_pairs_train.csv
-- drop table and.GS;
create table if not exists and.GS_train
(
    PMID1       String,
    Last_name1  String,
    Initials1   String,
    First_name1 String,
    PMID2       String,
    Last_name2  String,
    Initials2   String,
    First_name2 String,
    Authorship  String
) ENGINE = Log;

-- clickhouse-client --password root --port 9001 --format_csv_delimiter=';' --query='insert into and.GS_test FORMAT CSVWithNames' < ./400_pairs_test.csv
create table if not exists and.GS_test
(
    PMID1       String,
    Last_name1  String,
    Initials1   String,
    First_name1 String,
    PMID2       String,
    Last_name2  String,
    Initials2   String,
    First_name2 String,
    Authorship  String
) ENGINE = Log;


create view if not exists and.GS as
select *, 1 as Is_Train
from and.GS_train
union all
select *, 0 as Is_Train
from and.GS_test;

-- 1900
select count()
from and.GS;


-- ""	10
-- NO	687
-- N0	1
-- YES	1202
select Authorship, count() as cnt
from and.GS
group by Authorship;

-- array(toString(x),
-- JSONExtractString(authors_list_raw[x], 'lastName'),
-- JSONExtractString(authors_list_raw[x], 'foreName'),
-- JSONExtractString(authors_list_raw[x], 'initials')),

-- drop table and.GS_dataset1;
select is_train, count()
from and.GS_dataset
group by is_train;
create materialized view and.GS_dataset ENGINE = Log populate as
select pm_id1,
       lastname1,
       initials1,
       firstname1,
       pm_id2,
       lastname2,
       initials2,
       firstname2,
       same_author,
       is_train,
       author_list1,
       author_list2,
       arrayElement(arrayFilter(x -> length(x) > 0,
                                arrayMap(x->if(lower(x[2]) = lastname1 and lower(x[4]) = initials1, x[1], ''),
                                         author_list1)), 1)                      as matched_author_order1,
       arrayElement(arrayFilter(x -> length(x) > 0,
                                arrayMap(x->if(lower(x[2]) = lastname2 and lower(x[4]) = initials2, x[1], ''),
                                         author_list2)), 1)                      as matched_author_order2,
       lower(lastname1) in (select distinct(lastname) as lastanmes from and.top100_chinese_lastname) or
       lower(lastname2) in
       (select distinct(lastname) as lastanmes from and.top100_chinese_lastname) as is_chinese_lastname
from (

         select pm_id1,
                lastname1,
                initials1,
                firstname1,
                pm_id2,
                lastname2,
                initials2,
                firstname2,
                same_author,
                is_train,
                author_list1
         from (
                  select PMID1                                 as pm_id1,
                         lower(Last_name1)                     as lastname1,
                         lower(Initials1)                      as initials1,
                         lower(First_name1)                    as firstname1,
                         PMID2                                 as pm_id2,
                         lower(Last_name2)                     as lastname2,
                         lower(Initials2)                      as initials2,
                         lower(First_name2)                    as firstname2,
                         if(startsWith(Authorship, 'Y'), 1, 0) as same_author,
                         rand() % 10 <= 6 ? 1 : 0              as is_train -- left out 30% data for testing 原始GS数据集中已经拆分了测试集，但是比较小，测试稳定性太差，我们这里将其调大了一点
                  from and.GS
                  where length(Authorship) > 0) as t1 any
                  left join (select pm_id, author_list as author_list1 from and.nft_paper_author_name_list) as t2
                            on t1.pm_id1 = t2.pm_id) as t3 any
         left join (select pm_id, author_list as author_list2 from and.nft_paper_author_name_list) as t4
                   on t3.pm_id2 = t4.pm_id
where length(matched_author_order1) > 0
  and length(matched_author_order2) > 0;


-- 1729 通过lastname + initials的方式匹配到了1729个，剩余的100多个没有匹配到
select count()
from and.GS_dataset
where length(matched_author_order1) > 0
  and length(matched_author_order2) > 0;

--数据集3：SONG-balanced -------------------------------------------------------------------------------------------------------------------------------------------------
-- 统计数据集中的中文作者的个数和真实数据（pubmed authors）中的中文作者的个数，平衡两个数据集，并且融合两个数据集到一个跟真实数据集分布更接近的数据集

-- drop table and.top100_chinese_lastname;
-- clickhouse-client --password root --port 9001 --query='insert into and.top100_chinese_lastname FORMAT TSV' < and.top100_chinese_lastname.tsv
create table and.top100_chinese_lastname
(
    top_id   Int32,
    chinese  String,
    lastname String
) ENGINE = MergeTree partition by top_id > 0 order by top_id;

-- 100
select count()
from and.top100_chinese_lastname;

-- pubmed 中所有的作者 119781963
select sum(length(author_list)) as all_authors
from and.nft_paper_author_name_list;

-- pubmed 中类似中国姓名的作者 11663218 占整个pubmed文献数据库总量的比例是：9.737040292118104%
-- 0.09737040292118104	9.270061230099618
-- select 11663218/119781963, (119781963 - 11663218)/11663218;
-- pubmed 非中文作者:中文作者=9.270061230099618:1
select count()
from (
         select one_author[2] as lastname
         from and.nft_paper_author_name_list
                  array join author_list as one_author)
         any
         inner join
     (select distinct(lastname) as lastname from and.top100_chinese_lastname)
     using lastname;

-- SONG 数据集中的中文作者的占比 4.47%，远小于pubmed数据库中的中文作者比例，非中文作者：中文作者=21.346715328467152
-- [8220,183690]	4.474930589580271	21.346715328467152
select groupArray(cnt) as items, 100.0 * items[1] / items[2], (items[2] - items[1]) / items[1]
from (
      select count() as cnt
      from and.SONG_dataset
      where lower(lastname1) in (select distinct(lastname) as lastanmes from and.top100_chinese_lastname)
         or lower(lastname2) in (select distinct(lastname) as lastanmes from and.top100_chinese_lastname)
      union all
      select count() as cnt
      from and.SONG_dataset);

-- GS 数据集中的中文作者的占比 11.53%，接近pubmed数据库中的中文作者比例
-- [218,1890]	11.534391534391535
select groupArray(cnt) as items, 100.0 * items[1] / items[2]
from (
      select count() as cnt
      from and.GS_dataset
      where lower(lastname1) in (select distinct(lastname) as lastanmes from and.top100_chinese_lastname)
         or lower(lastname2) in (select distinct(lastname) as lastanmes from and.top100_chinese_lastname)
      union all
      select count() as cnt
      from and.GS_dataset);

-- SONG 精简的数据集，含有分布更加接近pubmed的中国学者比例，对非中文作者进行下采样
-- pubmed 非中文作者:中文作者=9.270061230099618:1
-- 83841	8220	75621	9.19963503649635
select count()                                                              as cnt,
       countEqual(groupArray(is_chinese_lastname) as sample_with_ethnic, 1) as chinese_last_name_cnt,
       countEqual(sample_with_ethnic, 0)                                    as non_chinese_last_name_cnt,
       non_chinese_last_name_cnt / chinese_last_name_cnt                    as pctg
from (
      select *,
             lower(lastname1) in (select distinct(lastname) as lastanmes from and.top100_chinese_lastname) or
             lower(lastname2) in
             (select distinct(lastname) as lastanmes from and.top100_chinese_lastname) as is_chinese_lastname
      from and.SONG_dataset
      where if(is_chinese_lastname,
               1, -- 含有中国人名
               rand() % 100 <= 100 * (9.270061230099618 / 21.346715328467152) -- 不含有中国人名
                )
         );

-- drop table and.SONG_dataset_balanced;
create materialized view if not exists and.SONG_dataset_balanced ENGINE = Log populate as
select *,
       lower(lastname1) in (select distinct(lastname) as lastanmes from and.top100_chinese_lastname) or
       lower(lastname2) in
       (select distinct(lastname) as lastanmes from and.top100_chinese_lastname) as is_chinese_lastname
from and.SONG_dataset
where if(is_chinese_lastname,
         1, -- 含有中国人名
         rand() % 100 <= 100 * (9.270061230099618 / 21.346715328467152) -- 不含有中国人名
          );

-- 85341
select count()
from and.SONG_dataset_balanced;

-- 85341	8220	77121	9.382116788321168
select count()                                                              as cnt,
       countEqual(groupArray(is_chinese_lastname) as sample_with_ethnic, 1) as chinese_last_name_cnt,
       countEqual(sample_with_ethnic, 0)                                    as non_chinese_last_name_cnt,
       non_chinese_last_name_cnt / chinese_last_name_cnt                    as pctg
from and.SONG_dataset_balanced;


--数据集4：Synthetic_dataset  -------------------------------------------------------------------------------------------------------------------------------------------------
-- drop table and.SYNTHETIC_dataset;
create view if not exists and.SYNTHETIC_dataset as
select *
from (select pm_id1,
             lastname1,
             initials1,
             pm_id2,
             lastname2,
             initials2,
             same_author,
             is_train,
             author_list1,
             author_list2,
             matched_author_order1,
             matched_author_order2,
             is_chinese_lastname
             -- 使用平衡的数据集
      from and.SONG_dataset_balanced
      union all
      select pm_id1,
             lastname1,
             initials1,
             pm_id2,
             lastname2,
             initials2,
             same_author,
             is_train,
             author_list1,
             author_list2,
             matched_author_order1,
             matched_author_order2,
             is_chinese_lastname
      from and.GS_dataset
      where length(matched_author_order1) > 0
        and length(matched_author_order2) > 0)
order by rand();

select count()
from and.GS_dataset;

-- 85055
select count()
from and.SYNTHETIC_dataset;

select same_author, count()
from and.SONG_dataset
group by same_author;

select same_author, count()
from and.GS_dataset
group by same_author;


select same_author, count()
from and.SONG_dataset_unbiased
group by same_author;

-- 数据集质量验证，是否是无偏数据集？ 已经将该数据集中的中文作者占比与pubmed协调一致了，TODO 是否还需要进一步的调整
-- pubmed 非中文作者:中文作者=9.270061230099618:1
-- 39620	3877	35743	9.219241681712665	1.839125761375851
select length(groupArray(is_chinese_lastname) as arr)                                             as all_cnt,
       countEqual(arr, 1)                                                                         as chinese_cnt,
       countEqual(arr, 0)                                                                         as non_chinese_cnt,
       non_chinese_cnt / chinese_cnt                                                              as non_ch_ch_ratio,
       countEqual(groupArray(same_author) as same_author_arr, 1) / countEqual(same_author_arr, 0) as pos_neg_ratio
from and.SONG_dataset_unbiased;


-- 重新生划分训练集和测试集，模拟交叉验证
-- drop table and.SONG_dataset_balanced_with_train_test_split;
create materialized view if not exists and.SONG_dataset_balanced_with_train_test_split ENGINE = Log populate as
select concat(pm_id1, '_', matched_author_order1, '|', pm_id2, '_', matched_author_order2)  as indi,
       arrayMap(x->rand(xxHash32(concat(indi, toString(x)))) % 10 <= 6 ? 1 : 0, range(200)) as is_train_arr,
       ns,
       ns_id_order_by_commoness_asc,
       commonness
from (select *, concat(lastname1, '_', substring(initials1, 1, 1)) as ns from and.SONG_dataset_balanced) any
         left join (select id as ns_id_order_by_commoness_asc, lastname_firat_initial as ns, commonness
                    from and.lastname_first_initial_commonness_with_id) using ns;


-- drop table and.SONG_dataset_with_train_test_split;
create materialized view if not exists and.SONG_dataset_with_train_test_split ENGINE = Log populate as
select indi,
       length(is_train_arr) == 200 ? is_train_arr : arrayFill(x->0, range(200)) as is_train_arr,
       ns,
       ns_id_order_by_commoness_asc,
       commonness
from (
         select concat(pm_id1, '_', matched_author_order1, '|', pm_id2, '_', matched_author_order2) as indi,
                ns,
                ns_id_order_by_commoness_asc,
                commonness
         from (select *, concat(lastname1, '_', substring(initials1, 1, 1)) as ns from and.SONG_dataset) any
                  left join (select id as ns_id_order_by_commoness_asc, lastname_firat_initial as ns, commonness
                             from and.lastname_first_initial_commonness_with_id) using ns
         ) any
         left join (select indi, is_train_arr from and.SONG_dataset_balanced_with_train_test_split)
                   using indi;

-- drop table and.GS_dataset_with_train_test_split;
create materialized view if not exists and.GS_dataset_with_train_test_split ENGINE = Log populate as
select concat(pm_id1, '_', matched_author_order1, '|', pm_id2, '_', matched_author_order2)  as indi,
       arrayMap(x->rand(xxHash32(concat(indi, toString(x)))) % 10 <= 6 ? 1 : 0, range(200)) as is_train_arr,
       ns,
       ns_id_order_by_commoness_asc,
       commonness
from (select *, concat(lastname1, '_', substring(initials1, 1, 1)) as ns from and.GS_dataset) any
         left join (select id as ns_id_order_by_commoness_asc, lastname_firat_initial as ns, commonness
                    from and.lastname_first_initial_commonness_with_id) using ns;

--  这里注意 合成数据集的训练集只能来自于 GS 和 SONG-balanced 的训练集，防止训练集泄露;
-- drop table and.SYNTHETIC_dataset_with_train_test_split;
create view if not exists and.SYNTHETIC_dataset_with_train_test_split as
select indi,
       any(is_train_arr)                 as is_train_arr,
       any(ns)                           as ns,
       any(ns_id_order_by_commoness_asc) as ns_id_order_by_commoness_asc,
       any(commonness)                   as commonness
from (
      select *
      from and.SONG_dataset_balanced_with_train_test_split
      union ALL
      select *
      from and.GS_dataset_with_train_test_split)
group by indi
;

-- drop table and.WHU_dataset_with_train_test_split;
create materialized view if not exists and.WHU_dataset_with_train_test_split ENGINE = Log populate as
select concat(pm_id1, '_', matched_author_order1, '|', pm_id2, '_', matched_author_order2)  as indi,
       arrayMap(x->rand(xxHash32(concat(indi, toString(x)))) % 10 <= 6 ? 1 : 0, range(200)) as is_train_arr,
       ns,
       ns_id_order_by_commoness_asc,
       commonness
from (select *, concat(lastname1, '_', substring(initials1, 1, 1)) as ns from and.WHU_dataset) any
         left join (select id as ns_id_order_by_commoness_asc, lastname_firat_initial as ns, commonness
                    from and.lastname_first_initial_commonness_with_id) using ns;


-- verify the positive/negative ratio
select arrayJoin(arrayMap(x->arrayCount(z->z == 1, arrayMap(y->y[x], groupArray(is_train_arr) as arr_arr) as col) /
                             arrayCount(z->z == 0, col), range(200))) as ratio
-- from and.WHU_dataset_with_train_test_split;
-- from and.SYNTHETIC_dataset_with_train_test_split;
from and.GS_dataset_with_train_test_split;
-- from and.SONG_dataset_with_train_test_split;
-- from and.SONG_dataset_balanced_with_train_test_split;

drop table and.WHU_dataset_with_train_test_split;
drop table and.SYNTHETIC_dataset_with_train_test_split;
drop table and.GS_dataset_with_train_test_split;
drop table and.SONG_dataset_with_train_test_split;
drop table and.SONG_dataset_balanced_with_train_test_split;

select groupArray(cnt), groupArray(cnt)[1] / sum(cnt)
from (
      select same_author, count() as cnt
      from and.SONG_dataset
      group by same_author)
;