select count()
from and.GS
where length(PMID1) == 0
   or length(PMID2) == 0;

-- drop table and.two_dataset_related_pubmed_paper;
create view if not exists and.two_dataset_related_pubmed_paper as
select *
from (
      (select pm_id, 'GS' as from
       from and.GS
                array join [PMID1, PMID2] as pm_id)
      union all
      (select toString(PMID) as pm_id, 'SONG' as from
       from and.SONG)
      union all
      (select pm_id, 'WHU' as from
       from and.WHU_dataset
                array join [pm_id1, pm_id2] as pm_id)
         )
where length(pm_id) > 0;


-- 合并获得的所有的作者ID
create materialized view if not exists and.available_pubmed_id_with_wellform_name ENGINE = Log populate as
select *
from (
         select *
         from (select *
               from and.pm_aminer_s2_mag_paper_mapping_with_wellform_name) any
                  left join (select concat(toString(PMID), '_', toString(au_order)) as pm_ao,
                                    toString(aid)                                   as pkg_aid_v2,
                                    toString(au_order)                              as pkg_aid_v2_author_order,
                                    concat(ForeName, ', ', LastName)                as pkg_aid_v2_author_name
                             from and.PKG_AuthorList_V2) using pm_ao) any
         left join (select concat(toString(PMID), '_', toString(au_order)) as pm_ao,
                           toString(Vetle_aid)                             as vetle_aid,
                           toString(StrongID)                              as pkg_aid_strong,
                           toString(aid)                                   as pkg_aid_v1
                    from and.PKG_AuthorList) using pm_ao;



-- 将两个黄金数据及涉及到的文章合并在一起
-- drop table and.EVAL_involved_pm_paper;
create materialized view if not exists and.EVAL_involved_pm_paper ENGINE = Log populate as
select *
from and.available_pubmed_id_with_wellform_name any
         inner join (
    select pm_id, groupUniqArray(from) as from
    from and.two_dataset_related_pubmed_paper
    group by pm_id) using pm_id;

select count(distinct pm_id)
from and.two_dataset_related_pubmed_paper;

-- 37193
select count()
from and.EVAL_involved_pm_paper;

-- 已经验证 s2_author_ids 中不存在一个作者两个ID的情况。所以后面可以使用 s2_author_ids[1] 取出作者ID
select count()
from and.EVAL_involved_pm_paper
where length(s2_author_ids) > 1;

-- 98146543
-- select * from and.mag_pm_id_author_id any left join mag.paper_author_affiliation using AffiliationId limit 500;
-- select count() from mag.paper_author_affiliation any inner join and.mag_pm_id_author_id using PaperId, AuthorId, AffiliationId, AuthorSequenceNumber, OriginalAuthor, OriginalAffiliation;
-- 60274932 of 98146543 相对于pubmed自身的affiliation，mag包含更多的
select count()
from and.mag_pm_id_author_id
where length(OriginalAffiliation) > 0;
select count()
from mag.affiliation;

-- 8913538 25%的pubmed文章有引文
select count()
from s2.semantic_scholar
where length(pmid) > 0
  and length(outCitations) > 0;

-- 14789084 443647769
-- 从mag中抽取出一些有利于 AND 任务的一些属性
-- MAG中pubmed 的那部分文章 paper-level

-- 1900
select count()
from and.GS;

create materialized view if not exists and.mag_EVAL_involved_pm_paper_author_affiliation ENGINE = Log populate as
select PaperId,
       AuthorId,
       AuthorSequenceNumber,
       AffiliationId,
       OriginalAffiliation,
       NormalizedName,
       DisplayName,
       GridId
from (
         select PaperId,
                AuthorId,
                AffiliationId,
                OriginalAffiliation,
                AuthorSequenceNumber
                -- 从 mag.paper_author_affiliation 过滤 存在于 黄金标准数据集 EVAL 中的 paper
         from mag.paper_author_affiliation any
                  inner join (select mag_paper_id as PaperId from and.EVAL_involved_pm_paper) using PaperId) any
         -- 根据 affiliation ID 匹配 原始的 affiliation
         left join (select AffiliationId, NormalizedName, DisplayName, GridId
                    from mag.affiliation) using AffiliationId;

-- drop table and.mag_EVAL_involved_pm_paper_author_affiliation;
-- 6139	35647	5.806646033555953
select count(distinct PaperId) as paper_cnt, count() as author_cnt, author_cnt / paper_cnt
from and.mag_EVAL_involved_pm_paper_author_affiliation;


create materialized view if not exists and.mag_EVAL_involved_pm_paper ENGINE = Log populate as
select PaperId,
       PaperTitle,
       OriginalTitle,
       Publisher,
       JournalId,
       ConferenceSeriesId,
       ConferenceInstanceId,
       OriginalVenue
from mag.paper any
         inner join (select mag_paper_id as PaperId from and.EVAL_involved_pm_paper) using PaperId;

-- 6139
select count()
from and.mag_EVAL_involved_pm_paper;

-- drop table and.mag_EVAL_involved_pm_paper_level_paper;
create materialized view if not exists and.mag_EVAL_involved_pm_paper_level_paper ENGINE = Log populate as
select PaperId                    as citing_mag_paper_id,
       CitedPaperId               as cited_mag_paper_id,
       author_id_with_order       as mag_author_id_with_orders,
       citing_author_affiliations as mag_citing_author_affiliations,
       CitingPaperInfo            as mag_citing_paper_info,
       CitedPaperInfo             as mag_cited_paper_info
from (
         select PaperId, CitedPaperId, author_id_with_order, citing_author_affiliations, CitingPaperInfo
         from (
                  select PaperId, CitedPaperId, author_id_with_order, citing_author_affiliations
                  from (
                           select PaperId, CitedPaperId, author_id_with_order
                           from (select PaperId, PaperReferenceId as CitedPaperId
                                 from mag.paper_reference) any
                                    inner join (select mag_paper_id                               as PaperId,
                                                       arraySort(x ->toUInt32(x[2]),
                                                                 groupArray(mag_author_id_order)) as author_id_with_order
                                                from (select mag_paper_id,
                                                             [toString(mag_author_id), toString(mag_author_order)] as mag_author_id_order
                                                             -- 从mag中的引文网络中找到 存在于黄金标准数据集 EVAL中的paper
                                                      from and.EVAL_involved_pm_paper
                                                      where mag_paper_id > 0)
                                                group by mag_paper_id) using PaperId
                           ) any
                           -- 为施引文献匹配其所有作者的 affiliation
                           left join (select PaperId,
                                             groupArray(citing_author_affiliation) as citing_author_affiliations
                                      from (select PaperId,
                                                   [toString(AuthorId), toString(AuthorSequenceNumber), toString(AffiliationId),
                                                       OriginalAffiliation, NormalizedName, DisplayName,
                                                       GridId] citing_author_affiliation
                                            from and.mag_EVAL_involved_pm_paper_author_affiliation)
                                      group by PaperId) using PaperId) any
                  left join (select PaperId,
                                    [PaperTitle, toString(JournalId), toString(ConferenceSeriesId),
                                        toString(ConferenceInstanceId), OriginalVenue] as CitingPaperInfo
                             from and.mag_EVAL_involved_pm_paper) using PaperId)
         any
         left join (select PaperId            as CitedPaperId,
                           [PaperTitle, toString(JournalId), toString(ConferenceSeriesId), toString(ConferenceInstanceId),
                               OriginalVenue] as CitedPaperInfo
                    from mag.paper) using CitedPaperId;

-- 165489
select count()
from and.mag_EVAL_involved_pm_paper_level_paper;

-- MAG中pubmed 的那部分作者 author-level
-- 在MAG 中， 包含EVAL数据集上的所有作者的所有文章，除了能够获得EVAL上的所有的作者，还能够获得这些作者的合作作者。
-- 11628314
-- select count()
-- from and.mag_EVAL_involved_pm_paper_level_author;
create materialized view if not exists and.mag_EVAL_involved_pm_paper_level_author ENGINE = Log populate as
select *
from (
      select AuthorId, PaperId, AuthorId, AffiliationId, AuthorSequenceNumber, OriginalAuthor, OriginalAffiliation
      from mag.paper_author_affiliation any
               inner join (
          select distinct(PaperId) as PaperId
          from (
                   select PaperId, AuthorId
                   from mag.paper_author_affiliation) any
                   inner join (select mag_paper_id as EVAL_PaperID, mag_author_id as AuthorId
                               from and.EVAL_involved_pm_paper
                               where AuthorId > 0) using AuthorId)
                          using PaperId);

-- 来自 Semantic Scholar 的黄金数据集上的数据
-- (title, paperAbstract, entities, fieldsOfStudy, authors, venue, journalName) as CitingPaperInfo,
-- drop table and.s2_EVAL_involved_pm_paper;
-- 51966
-- select count() from and.s2_EVAL_involved_pm_paper;
create materialized view if not exists and.s2_EVAL_involved_pm_paper ENGINE = Log populate as
select *
from s2.semantic_scholar any
         -- 从 s2 中过滤EVAL中涉及的论文
         inner join (
    select id
    from (
             select groupArray(arrayPushBack(outCitations, id)) as s2id_arr_of_arr
             from (select id, outCitations from s2.semantic_scholar) any
                      inner join (select s2_paper_id as id from and.EVAL_involved_pm_paper) using id
             )
             array join arrayFlatten(s2id_arr_of_arr) as id) using id;


-- 49815
select count()
from and.s2_EVAL_involved_pm_paper_level_paper;

create materialized view if not exists and.s2_EVAL_involved_pm_paper_level_paper ENGINE = Log populate as
select citing_s2_paper_id,
       cited_s2_paper_id,
       citing_paper_info as s2_citing_paper_info,
       cited_paper_info  as s2_cited_paper_info
from (
         select citing_s2_paper_id, cited_s2_paper_id, citing_paper_info
         from (
-- 构建EVAL数据集中的文章构成的引文网络
                  select id as citing_s2_paper_id, cited_s2_paper_id
                  from (
                           select id, outCitations
                           from (select id, outCitations from and.s2_EVAL_involved_pm_paper) any
                                    inner join (select s2_paper_id as id from and.EVAL_involved_pm_paper) using id
                           )
                           array join outCitations as cited_s2_paper_id)
                  -- 关联施引文献的信息
                  any
                  left join (select id            as citing_s2_paper_id,
                                    (title, paperAbstract, entities, fieldsOfStudy, authors, venue,
                                     journalName) as citing_paper_info
                             from and.s2_EVAL_involved_pm_paper) using citing_s2_paper_id)
         -- 关联被引文献的信息
         any
         left join (select id            as cited_s2_paper_id,
                           (title, paperAbstract, entities, fieldsOfStudy, authors, venue,
                            journalName) as cited_paper_info
                    from and.s2_EVAL_involved_pm_paper) using cited_s2_paper_id;


-- 476379239
-- select count() from and.s2_author_id;
create materialized view if not exists and.s2_author_id ENGINE = Log populate as
select s2_paper_id,
       pm_id,
       tupleElement(author, 1)                  as s2_author_order,
       tupleElement(tupleElement(author, 2), 1) as s2_author_name,
       tupleElement(tupleElement(author, 2), 2) as s2_author_ids
from (
         select id                                                   as s2_paper_id,
                pmid                                                 as pm_id,
                arrayMap(x-> JSONExtract(x, 'Tuple(name String, ids Array(String))'),
                         extractAll(authors, '\\{[^\\}]+\\}'))       as tmp,
                arrayMap(x -> tuple(x, tmp[x]), arrayEnumerate(tmp)) as author_list
         from s2.semantic_scholar)
         array join author_list as author;

-- s2 中的一个作者具有两个ID的数据条数是0， 意味着S2中所有的作者都是具有一个ID的
-- 0
-- select count() from and.s2_author_id where length(s2_author_ids) > 1;
-- 0
-- select count() from and.available_pubmed_id_with_wellform_name where length(s2_author_ids) > 1;

-- S2 中没有作者的affiliation
create materialized view if not exists and.s2_EVAL_involved_pm_paper_level_author ENGINE = Log populate as
select s2_paper_id, pm_id, s2_author_order, s2_author_name, s2_author_id
from (
      select s2_paper_id, pm_id, s2_author_order, s2_author_name, s2_author_ids[1] as s2_author_id
      from and.s2_author_id any
               inner join (
          select distinct(s2_paper_id) as s2_paper_id
          from (
                   select s2_paper_id, pm_id, s2_author_order, s2_author_name, s2_author_ids[1] as s2_author_id
                   from and.s2_author_id) any
                   inner join (select s2_paper_id, s2_author_ids[1] as s2_author_id
                               from and.EVAL_involved_pm_paper
                               where length(s2_author_ids) = 1) using s2_author_id)
                          using s2_paper_id);


-- 19250212
select count()
from and.s2_EVAL_involved_pm_paper_level_author;

-- 37193	6574
select count(), count(distinct pm_id) as distinct_paper_cnt
from and.EVAL_involved_pm_paper;

-- drop table and.EVAL_raw_dataset;
create materialized view if not exists and.EVAL_raw_dataset ENGINE = Log populate as
select *
from (
         select *
         from (
                  select *
                  from (select toString(pm_id) as pm_id,
                               journal,
                               article_title,
                               abstract_str,
                               authors,
                               languages,
                               publication_types,
                               vernacular_title,
                               suppl_meshs,
                               mesh_headings,
                               other_abstracts,
                               keywords,
                               references,
                               datetime_str
                        from pubmed.nft_paper) any
                           inner join (select pm_id,
                                              toString(min(mag_paper_id))                           as mag_paper_id,
                                              toString(min(s2_paper_id))                            as s2_paper_id,
                                              arraySort(x -> toUInt64OrZero(x[1]),
                                                        groupArray(mag_author_id_list_with_orders)) as mag_author_id_list_with_orders,
                                              arraySort(x -> toUInt64OrZero(x[1]),
                                                        groupArray(s2_author_id_list_with_orders))  as s2_author_id_list_with_orders,
                                              groupArray(pkg_aid_v1)                                as pkg_aid_v1_coauthors,
                                              groupArray(pkg_aid_v2)                                as pkg_aid_v2_coauthors,
                                              groupArray(pkg_aid_strong)                            as pkg_aid_strong_coauthors,
                                              groupArray(vetle_aid)                                 as vetle_aid_coauthors
                                       from (
                                             select pm_id,
                                                    mag_paper_id,
                                                    s2_paper_id,
                                                    pkg_aid_v1,
                                                    pkg_aid_v2,
                                                    pkg_aid_strong,
                                                    vetle_aid,
                                                    [mag_author_order, toString(mag_author_id)] as mag_author_id_list_with_orders,
                                                    [s2_author_order, s2_author_ids[1]]         as s2_author_id_list_with_orders
                                             from and.EVAL_involved_pm_paper)
                                       group by pm_id) using pm_id)
                  any
                  left join (select mag_paper_id,
                                    -- 已经验证了 groupUniqArray 之后数组的长度是1
                                    groupUniqArray(mag_citing_author_affiliations)[1] as mag_citing_author_affiliations,
                                    groupUniqArray(mag_citing_paper_info)[1]          as mag_citing_paper_info,
                                    tuple(groupArray(cited_item))                     as mag_cited_paper_infos
                             from (select toString(citing_mag_paper_id)                        as mag_paper_id,
--                                     mag_author_id_with_orders,
                                          mag_citing_author_affiliations,
                                          mag_citing_paper_info,
                                          (toString(cited_mag_paper_id), mag_cited_paper_info) as cited_item
                                   from and.mag_EVAL_involved_pm_paper_level_paper)
                             group by mag_paper_id) using mag_paper_id)
         any
         left join (select s2_paper_id,
                           -- 通过s2_paper_id聚合， s2_citing_paper_info 是唯一的
                           groupUniqArray(s2_citing_paper_info)[1] as s2_citing_paper_info,
                           tuple(groupArray(cited_item))           as s2_cited_paper_infos
                    from (select citing_s2_paper_id                       as s2_paper_id,
                                 s2_citing_paper_info,
                                 (cited_s2_paper_id, s2_cited_paper_info) as cited_item
                          from and.s2_EVAL_involved_pm_paper_level_paper)
                    group by s2_paper_id) using s2_paper_id;

desc and.EVAL_raw_dataset;
-- 6574
select count()
from and.EVAL_raw_dataset;

-- drop table and.EVAL_raw_dataset_with_clean_content;
create materialized view if not exists and.EVAL_raw_dataset_with_clean_content ENGINE = Log populate as
select *,
       arrayStringConcat(extractAll(CAST(if(article_title is NULL, '', article_title), 'String'), '\\w+'),
                         ' ') as clean_title,
       arrayStringConcat(arrayFilter(x -> x not in
                                          ('abstracttext', 'abstract') and
                                          not match(x, '\\d+'), splitByChar(' ',
                                                                            trimBoth(
                                                                                    replaceRegexpAll(
                                                                                            replaceRegexpAll(
                                                                                                    CAST(if(abstract_str is NULL, '', abstract_str), 'String'),
                                                                                                    '[^a-z]',
                                                                                                    ' '),
                                                                                            '\\s+',
                                                                                            ' '))
                                         )),
                         ' ') as clean_abstract,
       arrayStringConcat(arrayFilter(x -> x not in
                                          ('descriptorNameUI', 'descriptorName', 'majorTopicYN',
                                           'qualifierNameList',
                                           'false', 'true', 'null') and not match(x, '\\d+'),
                                     extractAll(CAST(if(mesh_headings is NULL, '', mesh_headings), 'String'), '\\w+')),
                         ' ') as clean_mesh_headings,
       arrayStringConcat(arrayFilter(x -> x not in
                                          ('keyword', 'majorTopicYN', 'false', 'true', 'null') and
                                          not match(x, '\\d+'),
                                     extractAll(CAST(if(keywords is NULL, '', keywords), 'String'), '\\w+')),
                         ' ') as clean_keywords
from and.EVAL_raw_dataset;


-- 已经验证了 groupUniqArray 之后数组的长度是1
-- 查询出来的是空 验证聚合查询的时候，其他相同的字段groupUniqArray()只会有有个结果。
select mag_paper_id,
       groupUniqArray(mag_citing_author_affiliations) as mag_citing_author_affiliations_arr,
       groupUniqArray(mag_citing_paper_info)          as mag_citing_paper_info_arr,
       tuple(groupArray(cited_item))                  as mag_cited_paper_info_arr
from (select toString(citing_mag_paper_id)                        as mag_paper_id,
--                                     mag_author_id_with_orders,
             mag_citing_author_affiliations,
             mag_citing_paper_info,
             (toString(cited_mag_paper_id), mag_cited_paper_info) as cited_item
      from and.mag_EVAL_involved_pm_paper_level_paper)
group by mag_paper_id
having length(mag_citing_author_affiliations_arr) > 1
    or length(mag_citing_paper_info_arr) > 1;
;

-- 已经验证了 groupUniqArray 之后数组的长度是1
select citing_s2_paper_id                   as s2_paper_id,
       groupUniqArray(s2_citing_paper_info) as x-- cited_s2_paper_id, s2_citing_paper_info, s2_cited_paper_info
from and.s2_EVAL_involved_pm_paper_level_paper
group by s2_paper_id
having length(x) > 1;


-- TODO 还需要添加是训练数据还是测试数据

-- 名字的流行程度 越流行的名字歧义性越大
create materialized view if not exists and.lastname_firat_initial_commonness ENGINE = Log populate as
select concat(author_name[2], '_', substring(author_name[4], 1, 1)) as lastname_firat_initial,
       count()                                                      as commonness
from and.nft_paper_author_name_list
         array join author_list as author_name
group by lastname_firat_initial
order by commonness desc;

-- 5802948
select count()
from and.lastname_firat_initial_commonness;

-- 测试chunk的切分方法 split namesapces to chunk, per chunk account for 1/1000 of entire namespaces
-- 119781963	0.0012261695861504624
select sum(commonness) as cnt, 146873 / cnt
from and.lastname_firat_initial_commonness;

-- 3527070 = 5802948 - 2275878
select count()
from and.lastname_firat_initial_commonness
where commonness > 1;

-- TODO 可能的特征，后面逐个增加特征
-- -- use machine leaning do disambiguate author
-- create table if not exists and.ml_and_raw_dataset
-- (
--     author_name            String,        -- Y
-- --     author_name_commonness String,
--     title                  String,        -- Y
--     keywords               String,        -- Y
--     abstract               String,        -- Y
--     meshheading            String,        -- Y
--     author_name_aff_list_with_orders              String,   -- Y 用来衡量两篇文章中是否有公共的作者
--     reference              String,        -- Y
--     aff_history      String,
--     pub_year               String,        -- Y
--     other_pub_pm_ids       Array(INT), -- 用来计算发表论文的数量
--
--     venue                  String,        -- Y
-- --     fieldsOfStudy         String,        -- from s2
--     mag_author_id_list_with_orders    String,        -- Y
--     s2_author_id_list_with_orders     String,        -- Y
--     language               String,        -- Y
-- --     entities                 Array(String) -- from s2
--
--     citing_mag_paper_id,
--     cited_mag_paper_id,
--     citing_s2_paper_id,
--     cited_s2_paper_id,
--
-- --     mag_author_id_with_orders,
--     mag_citing_author_affiliations,
--
--     mag_citing_paper_info,
--     mag_cited_paper_info,
--     citing_paper_info,
--     cited_paper_info
-- );


-- 创建该表的目的是为了更好的将namespace划分为chunk
-- 增加 commonness特征
-- drop table and.lastname_first_initial_commonness_with_id;
create materialized view if not exists and.lastname_first_initial_commonness_with_id ENGINE = MergeTree(pseudo_date, id, 128) populate as
select rowNumberInAllBlocks() + 1 as id, -- increment id
       lastname_firat_initial,
       commonness,
       pseudo_date
from (select *, toDate('2020-01-01') as pseudo_date
      from and.lastname_firat_initial_commonness
      where length(lastname_firat_initial) > 1
      order by commonness asc);

-- 5802947
select count()
from and.lastname_first_initial_commonness_with_id;


-- 添加特征：给pubmed中的所有作者加上 ns_commonness id, 增加了PKG authorID
create materialized view if not exists and.nft_paper_author_ns_commonness_with_id ENGINE = MergeTree(pseudo_date, id, 128) populate as
select pm_ao,
       pm_id,
       datetime_str,
       one_author,
       pm_author_order,
       aminer_paper_id,
       aminer_author_order,
       aminer_author_id,
       s2_paper_id,
       s2_author_order,
       s2_author_name,
       s2_author_ids,
       mag_paper_id,
       mag_author_order,
       mag_author_name,
       mag_author_id,
       pkg_aid_v2_author_order,
       pkg_aid_v2_author_name,
       pkg_aid_v2,
       pkg_aid_v1,
       pkg_aid_strong,
       vetle_aid,
       lastname_firat_initial,
       id,
       commonness,
       pseudo_date
from (select *, concat(one_author[2], '_', substring(one_author[4], 1, 1)) as lastname_firat_initial
      from and.available_pubmed_id_with_wellform_name) any
         left join and.lastname_first_initial_commonness_with_id using lastname_firat_initial;


select max(id)
from and.nft_paper_author_ns_commonness_with_id;
-- 225449 of 119781963
select count()
from and.nft_paper_author_ns_commonness_with_id
where id = 4500000;


-- 跨库引用，引用fp.paper_clean_content，建表语句如下
-- create materialized view if not exists fp.paper_clean_content ENGINE = Log populate as
-- select pm_id,
--        arrayStringConcat(extractAll(CAST(if(article_title is NULL, '', article_title), 'String'), '\\w+'),
--                          ' ') as clean_title,
--        arrayStringConcat(arrayFilter(x -> x not in
--                                           ('abstracttext', 'abstract') and
--                                           not match(x, '\\d+'), splitByChar(' ',
--                                                                             trimBoth(
--                                                                                     replaceRegexpAll(
--                                                                                             replaceRegexpAll(
--                                                                                                     CAST(if(abstract_str is NULL, '', abstract_str), 'String'),
--                                                                                                     '[^a-z]',
--                                                                                                     ' '),
--                                                                                             '\\s+',
--                                                                                             ' '))
--                                          )),
--                          ' ') as clean_abstract,
--        arrayStringConcat(arrayFilter(x -> x not in
--                                           ('descriptorNameUI', 'descriptorName', 'majorTopicYN',
--                                            'qualifierNameList',
--                                            'false', 'true', 'null') and not match(x, '\\d+'),
--                                      extractAll(CAST(if(mesh_headings is NULL, '', mesh_headings), 'String'), '\\w+')),
--                          ' ') as clean_mesh_headings,
--        arrayStringConcat(arrayFilter(x -> x not in
--                                           ('keyword', 'majorTopicYN', 'false', 'true', 'null') and
--                                           not match(x, '\\d+'),
--                                      extractAll(CAST(if(keywords is NULL, '', keywords), 'String'), '\\w+')),
--                          ' ') as clean_keywords,
--        datetime_str
-- from pubmed.nft_paper;

-- 30419647
select count()
from fp.paper_clean_content;


-- journal, -- article_title, -- abstract_str,
-- authors, -- languages, -- publication_types,
-- vernacular_title, -- suppl_meshs, -- mesh_headings,
-- other_abstracts, -- keywords, -- references, -- datetime_str
-- drop table and.nft_paper_author_and_dataset_with_ns_id;
-- 添加特征：增加特征clean content特征 给pubmed中的作者加上
create materialized view if not exists and.nft_paper_author_and_dataset_with_ns_id ENGINE = MergeTree(pseudo_date, id, 128) populate as
select *
from and.nft_paper_author_ns_commonness_with_id any
         left join (select toString(pm_id)                         as pm_id,
                           clean_title,
--                            clean_abstract,
                           clean_mesh_headings,
                           clean_keywords,
                           toUInt32(substring(datetime_str, 1, 4)) as pub_year
                    from fp.paper_clean_content) using pm_id;


-- drop table and.nft_paper_co_authors;
create materialized view if not exists and.nft_paper_co_authors ENGINE = Log populate as
select pm_id,
       groupArray(lastname_firat_initial) as co_author_name,
       groupArray(aminer_author_id)       as aminer_co_author_id,
       groupArray(mag_author_id)          as mag_co_author_id,
       groupArray(s2_author_ids[1])       as s2_co_author_id,
       groupArray(pkg_aid_v2)             as pkg_co_author_id,
       groupArray(pkg_aid_v1)             as pkg_v1_co_author_id,
--        groupArray(pkg_aid_strong)         as pkg_strong_co_author_id,
       groupArray(vetle_aid)              as vetle_co_author_id
from and.nft_paper_author_ns_commonness_with_id
group by pm_id;

-- 添加特征：增加coauthor特征
-- drop table and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn;
create materialized view if not exists and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn ENGINE = MergeTree(pseudo_date, id, 128) populate as
select *,
       arrayReverseSort(x -> length(x),
                        splitByChar(' ', replaceAll(one_author[3], '-', '')) as fn_split)[1] as fn_max_len_str,
       position(one_author[3], '-') > 0                                                      as fn_contain_dash,
       length(replaceRegexpAll(one_author[3], '[a-z|\\-| ]', '')) > 0                        as fn_contain_other_chars
from and.nft_paper_author_and_dataset_with_ns_id any
         left join and.nft_paper_co_authors using pm_id;

-- 8	49	176	162	18
--  pmid的最大长度是8	文章的作者的最多个数是49
--  pmid的最大长度是8	文章的作者的最多个数是49
select max(length(pm_id))            as max_length_pm_id_str,
       max(toUInt32(author_name[1])) as max_count_author,
       max(length(author_name[2]))   as max_length_lastname,
       max(length(author_name[3]))   as max_length_firstname,
       max(length(author_name[4]))   as max_length_initials -- only 458 authors >6
from and.nft_paper_author_name_list
         array join author_list as author_name;

-- 133493
select count()
from and.nft_paper_author_name_list
         array join author_list as author_name
where length(author_name[2]) > 20;

-- 统计出现的最多特殊字符（的非 a-z 字符）
-- - é í á ü ö ó ç ł ú . ø ã éé è ë -ç ' å éè ş ô éô â š -é .. ï ê áó ä ñ ı ğ éí ż íé î ̇ ò ž -ü áš à é- ří -éè ř áá üü --
-- ć öü ě í- éá çã íú éó -ë íá -ö -è ç- áé üş æ şü õ íó -å íí ė ő çğ ū ţ ış áí łł éú -éé í-é č ö- úé ăă éã ıı şı üç éï éç
-- áã øø -í đ öç ă óá ... .-. ý ª íã ň áç ïé óí -á çı -. ü- úí éë -ø íç ôú ôé ãé úá çğı üö ãí é-í áú şüü šá óé í-ú -î ä-
-- łż áô а á- íñ ę ø- éê éé- ð éâ ì ç-é ôá ā ğç ĕ šć ıç øæ íê šěá ãá ııı áê ľ óó íô -ó üğ čć é. çğş ūė -ï ù û '' óã ľí üı
-- öé úš öö ç-ï öğ ī úã ț úñ é-á å- ń ] 'í ğş ó- şş -ú áçã áâ -áó èé í-á ë- ș ãú úó è- ãç -ã ē üçü о íě đđ ãã şö öş м ôí
-- ãê êé --- øå úç áñ åø şğ ãó âç ğı ıö ãô őő ̇ı âú ʼ é' ıü üşü êã -' 'á ää žě üüö ō çç ôç éñ éçã ªé é-é е / ś íóã ú- ã- âé
-- äö şç âá ééé íð üüü é-ó úú α ̇ö óú èè éáó 'é ñó üış žć ğü êá ááó í-ó ūėšė âí ªá íâ ôã í-- öó éõ '-' ̇ç ê- ěá ôó íõ ôõ âã
-- óö ěž öı àò íçã ̇̇ şıı ι т óż åå âê ÿ á-á ̇ü üçğ áóá čí ĭ úê ̇ş '- úçã ӧ ćć ìá -ñ í. -ê çé ľš šč üöü í-í .... çõ óô ì-á çö
-- çş ááá óç óöé čá ş- éáçã ü̇ ãâ ñí ııç žė öá óñ аа ç-ë âó ªú ε öüş çá ăţ ç-éô ñ- üüş -à μ áü í' ığ řš ñá ĺ ö-ü -š -é- éè-
-- -éô -ăă áö íü ź şăţă ú-í êç κ -č öüö ё â- áóé öüü ôê œ íéí ñé в ô- ö̇ šū íö á-í ß ş̇ і ď šž к ĵ îş üşğ ïç žč řá , ëé âéú
-- éíí óê éü ðú ν é-ë å-ø ééè -ţ åö éö ŕ öüç üü̇ óð šė -ô öüüü éª üıı ï- çü -ä -ş æø ãôê ôâ еа ̇ğ ă- ţ-ţ í-ñ ú'é -â ó-í é-ú
-- éî î- áõ ǧ éĵ áň êçã çığı üşı
select count() as cnt, other_chars_in_firstname
from (
      select paper_author_name[4]                       as firstname,
             replaceRegexpAll(firstname, '[a-z| ]', '') as other_chars_in_firstname
      from and.nft_paper_author_name_in_one_string
      where length(other_chars_in_firstname) > 0)
group by other_chars_in_firstname
order by cnt desc;

--收集包含特征的数据，使用外部程序python 或者 java 抽取特征并且进行模型评估--------------------------------------------------------------------------------------------------------------------------------------------------
-- drop table and.SONG_dataset_rich_features;
create materialized view if not exists and.SONG_dataset_rich_features ENGINE = Log populate as
select *
from (
         select *, concat(pm_id2, '_', matched_author_order2) as pm_ao2
         from (
                  select *, concat(pm_id1, '_', matched_author_order1) as pm_ao1
                  from (
                           select *
                           from (
                                    select *,
                                           concat(lastname1, '_', substring(initials1, 1, 1)) as lastname_firat_initial1,
                                           concat(lastname2, '_', substring(initials2, 1, 1)) as lastname_firat_initial2
                                    from (
                                             select *,
                                                    same_author,
                                                    author_list1[toUInt32(matched_author_order1)][3] as firstname1,
                                                    author_list2[toUInt32(matched_author_order2)][3] as firstname2
                                             from and.SONG_dataset
                                                      any
                                                      left join (select pm_id                          as pm_id1,
                                                                        journal                        as journal1,
                                                                        article_title                  as article_title1,
                                                                        abstract_str                   as abstract_str1,
                                                                        authors                        as authors1,
                                                                        languages                      as languages1,
                                                                        publication_types              as publication_types1,
                                                                        vernacular_title               as vernacular_title1,
                                                                        suppl_meshs                    as suppl_meshs1,
                                                                        mesh_headings                  as mesh_headings1,
                                                                        other_abstracts                as other_abstracts1,
                                                                        keywords                       as keywords1,
                                                                        references                     as references1,
                                                                        datetime_str                   as datetime_str1,
--                       mag_paper_id as mag_paper_id1,
                                                                        mag_author_id_list_with_orders as mag_author_id_list_with_orders1,
                                                                        s2_author_id_list_with_orders  as s2_author_id_list_with_orders1,
                                                                        mag_citing_author_affiliations as mag_citing_author_affiliations1,
                                                                        mag_citing_paper_info          as mag_citing_paper_info1,
                                                                        mag_cited_paper_infos          as mag_cited_paper_info1,
--                       s2_paper_id as s2_paper_id1,
                                                                        s2_citing_paper_info           as s2_citing_paper_info1,
                                                                        s2_cited_paper_infos           as s2_cited_paper_info1,

                                                                        pkg_aid_v1_coauthors           as pkg_aid_v1_coauthors1,
                                                                        pkg_aid_v2_coauthors           as pkg_aid_v2_coauthors1,
                                                                        pkg_aid_strong_coauthors       as pkg_aid_strong_coauthors1,
                                                                        vetle_aid_coauthors            as vetle_aid_coauthors1,

                                                                        clean_title                    as clean_title1,
                                                                        clean_keywords                 as clean_keywords1,
                                                                        clean_mesh_headings            as clean_mesh_headings1,
                                                                        clean_abstract                 as clean_abstract1
                                                                 from and.EVAL_raw_dataset_with_clean_content)
                                                                using pm_id1) any
                                             left join (select pm_id                          as pm_id2,
                                                               journal                        as journal2,
                                                               article_title                  as article_title2,
                                                               abstract_str                   as abstract_str2,
                                                               authors                        as authors2,
                                                               languages                      as languages2,
                                                               publication_types              as publication_types2,
                                                               vernacular_title               as vernacular_title2,
                                                               suppl_meshs                    as suppl_meshs2,
                                                               mesh_headings                  as mesh_headings2,
                                                               other_abstracts                as other_abstracts2,
                                                               keywords                       as keywords2,
                                                               references                     as references2,
                                                               datetime_str                   as datetime_str2,
--                                                                                                          mag_paper_id as mag_paper_id2,
                                                               mag_author_id_list_with_orders as mag_author_id_list_with_orders2,
                                                               s2_author_id_list_with_orders  as s2_author_id_list_with_orders2,
                                                               mag_citing_author_affiliations as mag_citing_author_affiliations2,
                                                               mag_citing_paper_info          as mag_citing_paper_info2,
                                                               mag_cited_paper_infos          as mag_cited_paper_info2,
--                                                                                                          s2_paper_id as s2_paper_id2,
                                                               s2_citing_paper_info           as s2_citing_paper_info2,
                                                               s2_cited_paper_infos           as s2_cited_paper_info2,

                                                               pkg_aid_v1_coauthors           as pkg_aid_v1_coauthors2,
                                                               pkg_aid_v2_coauthors           as pkg_aid_v2_coauthors2,
                                                               pkg_aid_strong_coauthors       as pkg_aid_strong_coauthors2,
                                                               vetle_aid_coauthors            as vetle_aid_coauthors2,

                                                               clean_title                    as clean_title2,
                                                               clean_keywords                 as clean_keywords2,
                                                               clean_mesh_headings            as clean_mesh_headings2,
                                                               clean_abstract                 as clean_abstract2
                                                        from and.EVAL_raw_dataset_with_clean_content) using pm_id2) any
                                    left join (select lastname_firat_initial as lastname_firat_initial1,
                                                      commonness             as name_commonness1
                                               from and.lastname_firat_initial_commonness) using lastname_firat_initial1)
                           any
                           left join (select lastname_firat_initial as lastname_firat_initial2,
                                             commonness             as name_commonness2
                                      from and.lastname_firat_initial_commonness) using lastname_firat_initial2) any
                  left join (select pm_ao               as pm_ao1,
                                    one_author          as one_author1,
                                    aminer_paper_id     as aminer_paper_id1,
                                    aminer_author_order as aminer_author_order1,
                                    aminer_author_id    as aminer_author_id1,
                                    s2_paper_id         as s2_paper_id1,
                                    s2_author_order     as s2_author_order1,
                                    s2_author_ids[1]    as s2_author_id1,
                                    mag_paper_id        as mag_paper_id1,
                                    mag_author_order    as mag_author_order1,
                                    mag_author_id       as mag_author_id1,

                                    vetle_aid           as vetle_aid1,
                                    pkg_aid_strong      as pkg_aid_strong1,
                                    pkg_aid_v1          as pkg_aid_v11,
                                    pkg_aid_v2          as pkg_aid_v21,

                                    from                as from1
                             from and.EVAL_involved_pm_paper) using pm_ao1) any
         left join (select pm_ao               as pm_ao2,
                           one_author          as one_author2,
                           aminer_paper_id     as aminer_paper_id2,
                           aminer_author_order as aminer_author_order2,
                           aminer_author_id    as aminer_author_id2,
                           s2_paper_id         as s2_paper_id2,
                           s2_author_order     as s2_author_order2,
                           s2_author_ids[1]    as s2_author_id2,
                           mag_paper_id        as mag_paper_id2,
                           mag_author_order    as mag_author_order2,
                           mag_author_id       as mag_author_id2,

                           vetle_aid           as vetle_aid2,
                           pkg_aid_strong      as pkg_aid_strong2,
                           pkg_aid_v1          as pkg_aid_v12,
                           pkg_aid_v2          as pkg_aid_v22,

                           from                as from2
                    from and.EVAL_involved_pm_paper) using pm_ao2;

-- 183690
select count()
from and.SONG_dataset_rich_features;


-- drop table and.SONG_dataset_balanced_rich_features;
create materialized view if not exists and.SONG_dataset_balanced_rich_features ENGINE = Log populate as
select *
from ( select *, concat(pm_id2, '_', matched_author_order2) as pm_ao2
       from (
                select *, concat(pm_id1, '_', matched_author_order1) as pm_ao1
                from (
                         select *
                         from (
                                  select *,
                                         concat(lastname1, '_', substring(initials1, 1, 1)) as lastname_firat_initial1,
                                         concat(lastname2, '_', substring(initials2, 1, 1)) as lastname_firat_initial2
                                  from (
                                           select *,
                                                  same_author,
                                                  author_list1[toUInt32(matched_author_order1)][3] as firstname1,
                                                  author_list2[toUInt32(matched_author_order2)][3] as firstname2
                                           from and.SONG_dataset_balanced
                                                    any
                                                    left join (select pm_id                          as pm_id1,
                                                                      journal                        as journal1,
                                                                      article_title                  as article_title1,
                                                                      abstract_str                   as abstract_str1,
                                                                      authors                        as authors1,
                                                                      languages                      as languages1,
                                                                      publication_types              as publication_types1,
                                                                      vernacular_title               as vernacular_title1,
                                                                      suppl_meshs                    as suppl_meshs1,
                                                                      mesh_headings                  as mesh_headings1,
                                                                      other_abstracts                as other_abstracts1,
                                                                      keywords                       as keywords1,
                                                                      references                     as references1,
                                                                      datetime_str                   as datetime_str1,
--                       mag_paper_id as mag_paper_id1,
                                                                      mag_author_id_list_with_orders as mag_author_id_list_with_orders1,
                                                                      s2_author_id_list_with_orders  as s2_author_id_list_with_orders1,
                                                                      mag_citing_author_affiliations as mag_citing_author_affiliations1,
                                                                      mag_citing_paper_info          as mag_citing_paper_info1,
                                                                      mag_cited_paper_infos          as mag_cited_paper_info1,
--                       s2_paper_id as s2_paper_id1,
                                                                      s2_citing_paper_info           as s2_citing_paper_info1,
                                                                      s2_cited_paper_infos           as s2_cited_paper_info1,

                                                                      pkg_aid_v1_coauthors           as pkg_aid_v1_coauthors1,
                                                                      pkg_aid_v2_coauthors           as pkg_aid_v2_coauthors1,
                                                                      pkg_aid_strong_coauthors       as pkg_aid_strong_coauthors1,
                                                                      vetle_aid_coauthors            as vetle_aid_coauthors1,

                                                                      clean_title                    as clean_title1,
                                                                      clean_keywords                 as clean_keywords1,
                                                                      clean_mesh_headings            as clean_mesh_headings1,
                                                                      clean_abstract                 as clean_abstract1
                                                               from and.EVAL_raw_dataset_with_clean_content)
                                                              using pm_id1) any
                                           left join (select pm_id                          as pm_id2,
                                                             journal                        as journal2,
                                                             article_title                  as article_title2,
                                                             abstract_str                   as abstract_str2,
                                                             authors                        as authors2,
                                                             languages                      as languages2,
                                                             publication_types              as publication_types2,
                                                             vernacular_title               as vernacular_title2,
                                                             suppl_meshs                    as suppl_meshs2,
                                                             mesh_headings                  as mesh_headings2,
                                                             other_abstracts                as other_abstracts2,
                                                             keywords                       as keywords2,
                                                             references                     as references2,
                                                             datetime_str                   as datetime_str2,
--                                                                                                          mag_paper_id as mag_paper_id2,
                                                             mag_author_id_list_with_orders as mag_author_id_list_with_orders2,
                                                             s2_author_id_list_with_orders  as s2_author_id_list_with_orders2,
                                                             mag_citing_author_affiliations as mag_citing_author_affiliations2,
                                                             mag_citing_paper_info          as mag_citing_paper_info2,
                                                             mag_cited_paper_infos          as mag_cited_paper_info2,
--                                                                                                          s2_paper_id as s2_paper_id2,
                                                             s2_citing_paper_info           as s2_citing_paper_info2,
                                                             s2_cited_paper_infos           as s2_cited_paper_info2,

                                                             pkg_aid_v1_coauthors           as pkg_aid_v1_coauthors2,
                                                             pkg_aid_v2_coauthors           as pkg_aid_v2_coauthors2,
                                                             pkg_aid_strong_coauthors       as pkg_aid_strong_coauthors2,
                                                             vetle_aid_coauthors            as vetle_aid_coauthors2,

                                                             clean_title                    as clean_title2,
                                                             clean_keywords                 as clean_keywords2,
                                                             clean_mesh_headings            as clean_mesh_headings2,
                                                             clean_abstract                 as clean_abstract2
                                                      from and.EVAL_raw_dataset_with_clean_content) using pm_id2) any
                                  left join (select lastname_firat_initial as lastname_firat_initial1,
                                                    commonness             as name_commonness1
                                             from and.lastname_firat_initial_commonness) using lastname_firat_initial1)
                         any
                         left join (select lastname_firat_initial as lastname_firat_initial2,
                                           commonness             as name_commonness2
                                    from and.lastname_firat_initial_commonness) using lastname_firat_initial2) any
                left join (select pm_ao               as pm_ao1,
                                  one_author          as one_author1,
                                  aminer_paper_id     as aminer_paper_id1,
                                  aminer_author_order as aminer_author_order1,
                                  aminer_author_id    as aminer_author_id1,
                                  s2_paper_id         as s2_paper_id1,
                                  s2_author_order     as s2_author_order1,
                                  s2_author_ids[1]    as s2_author_id1,
                                  mag_paper_id        as mag_paper_id1,
                                  mag_author_order    as mag_author_order1,
                                  mag_author_id       as mag_author_id1,

                                  vetle_aid           as vetle_aid1,
                                  pkg_aid_strong      as pkg_aid_strong1,
                                  pkg_aid_v1          as pkg_aid_v11,
                                  pkg_aid_v2          as pkg_aid_v21,

                                  from                as from1
                           from and.EVAL_involved_pm_paper) using pm_ao1) any
         left join (select pm_ao               as pm_ao2,
                           one_author          as one_author2,
                           aminer_paper_id     as aminer_paper_id2,
                           aminer_author_order as aminer_author_order2,
                           aminer_author_id    as aminer_author_id2,
                           s2_paper_id         as s2_paper_id2,
                           s2_author_order     as s2_author_order2,
                           s2_author_ids[1]    as s2_author_id2,
                           mag_paper_id        as mag_paper_id2,
                           mag_author_order    as mag_author_order2,
                           mag_author_id       as mag_author_id2,

                           vetle_aid           as vetle_aid2,
                           pkg_aid_strong      as pkg_aid_strong2,
                           pkg_aid_v1          as pkg_aid_v12,
                           pkg_aid_v2          as pkg_aid_v22,

                           from                as from2
                    from and.EVAL_involved_pm_paper) using pm_ao2
;

-- 85341
select count()
from and.SONG_dataset_balanced_rich_features;


-- drop table and.GS_dataset_rich_features;
create materialized view if not exists and.GS_dataset_rich_features ENGINE = Log populate as
select *
from ( select *, concat(pm_id2, '_', matched_author_order2) as pm_ao2
       from (
                select *, concat(pm_id1, '_', matched_author_order1) as pm_ao1
                from (
                         select *
                         from (
                                  select *,
                                         concat(lastname1, '_', substring(initials1, 1, 1)) as lastname_firat_initial1,
                                         concat(lastname2, '_', substring(initials2, 1, 1)) as lastname_firat_initial2
                                  from (
                                           select *
                                           from and.GS_dataset
                                                    any
                                                    left join (select pm_id                          as pm_id1,
                                                                      journal                        as journal1,
                                                                      article_title                  as article_title1,
                                                                      abstract_str                   as abstract_str1,
                                                                      authors                        as authors1,
                                                                      languages                      as languages1,
                                                                      publication_types              as publication_types1,
                                                                      vernacular_title               as vernacular_title1,
                                                                      suppl_meshs                    as suppl_meshs1,
                                                                      mesh_headings                  as mesh_headings1,
                                                                      other_abstracts                as other_abstracts1,
                                                                      keywords                       as keywords1,
                                                                      references                     as references1,
                                                                      datetime_str                   as datetime_str1,
--                       mag_paper_id as mag_paper_id1,
                                                                      mag_author_id_list_with_orders as mag_author_id_list_with_orders1,
                                                                      s2_author_id_list_with_orders  as s2_author_id_list_with_orders1,
                                                                      mag_citing_author_affiliations as mag_citing_author_affiliations1,
                                                                      mag_citing_paper_info          as mag_citing_paper_info1,
                                                                      mag_cited_paper_infos          as mag_cited_paper_info1,
--                       s2_paper_id as s2_paper_id1,
                                                                      s2_citing_paper_info           as s2_citing_paper_info1,
                                                                      s2_cited_paper_infos           as s2_cited_paper_info1,

                                                                      pkg_aid_v1_coauthors           as pkg_aid_v1_coauthors1,
                                                                      pkg_aid_v2_coauthors           as pkg_aid_v2_coauthors1,
                                                                      pkg_aid_strong_coauthors       as pkg_aid_strong_coauthors1,
                                                                      vetle_aid_coauthors            as vetle_aid_coauthors1,

                                                                      clean_title                    as clean_title1,
                                                                      clean_keywords                 as clean_keywords1,
                                                                      clean_mesh_headings            as clean_mesh_headings1,
                                                                      clean_abstract                 as clean_abstract1
                                                               from and.EVAL_raw_dataset_with_clean_content)
                                                              using pm_id1) any
                                           left join (select pm_id                          as pm_id2,
                                                             journal                        as journal2,
                                                             article_title                  as article_title2,
                                                             abstract_str                   as abstract_str2,
                                                             authors                        as authors2,
                                                             languages                      as languages2,
                                                             publication_types              as publication_types2,
                                                             vernacular_title               as vernacular_title2,
                                                             suppl_meshs                    as suppl_meshs2,
                                                             mesh_headings                  as mesh_headings2,
                                                             other_abstracts                as other_abstracts2,
                                                             keywords                       as keywords2,
                                                             references                     as references2,
                                                             datetime_str                   as datetime_str2,
--                                                                                                          mag_paper_id as mag_paper_id2,
                                                             mag_author_id_list_with_orders as mag_author_id_list_with_orders2,
                                                             s2_author_id_list_with_orders  as s2_author_id_list_with_orders2,
                                                             mag_citing_author_affiliations as mag_citing_author_affiliations2,
                                                             mag_citing_paper_info          as mag_citing_paper_info2,
                                                             mag_cited_paper_infos          as mag_cited_paper_info2,
--                                                                                                          s2_paper_id as s2_paper_id2,
                                                             s2_citing_paper_info           as s2_citing_paper_info2,
                                                             s2_cited_paper_infos           as s2_cited_paper_info2,

                                                             pkg_aid_v1_coauthors           as pkg_aid_v1_coauthors2,
                                                             pkg_aid_v2_coauthors           as pkg_aid_v2_coauthors2,
                                                             pkg_aid_strong_coauthors       as pkg_aid_strong_coauthors2,
                                                             vetle_aid_coauthors            as vetle_aid_coauthors2,

                                                             clean_title                    as clean_title2,
                                                             clean_keywords                 as clean_keywords2,
                                                             clean_mesh_headings            as clean_mesh_headings2,
                                                             clean_abstract                 as clean_abstract2
                                                      from and.EVAL_raw_dataset_with_clean_content) using pm_id2) any
                                  left join (select lastname_firat_initial as lastname_firat_initial1,
                                                    commonness             as name_commonness1
                                             from and.lastname_firat_initial_commonness) using lastname_firat_initial1)
                         any
                         left join (select lastname_firat_initial as lastname_firat_initial2,
                                           commonness             as name_commonness2
                                    from and.lastname_firat_initial_commonness) using lastname_firat_initial2) any
                left join (select pm_ao               as pm_ao1,
                                  one_author          as one_author1,
                                  aminer_paper_id     as aminer_paper_id1,
                                  aminer_author_order as aminer_author_order1,
                                  aminer_author_id    as aminer_author_id1,
                                  s2_paper_id         as s2_paper_id1,
                                  s2_author_order     as s2_author_order1,
                                  s2_author_ids[1]    as s2_author_id1,
                                  mag_paper_id        as mag_paper_id1,
                                  mag_author_order    as mag_author_order1,
                                  mag_author_id       as mag_author_id1,

                                  vetle_aid           as vetle_aid1,
                                  pkg_aid_strong      as pkg_aid_strong1,
                                  pkg_aid_v1          as pkg_aid_v11,
                                  pkg_aid_v2          as pkg_aid_v21,

                                  from                as from1
                           from and.EVAL_involved_pm_paper) using pm_ao1) any
         left join (select pm_ao               as pm_ao2,
                           one_author          as one_author2,
                           aminer_paper_id     as aminer_paper_id2,
                           aminer_author_order as aminer_author_order2,
                           aminer_author_id    as aminer_author_id2,
                           s2_paper_id         as s2_paper_id2,
                           s2_author_order     as s2_author_order2,
                           s2_author_ids[1]    as s2_author_id2,
                           mag_paper_id        as mag_paper_id2,
                           mag_author_order    as mag_author_order2,
                           mag_author_id       as mag_author_id2,

                           vetle_aid           as vetle_aid2,
                           pkg_aid_strong      as pkg_aid_strong2,
                           pkg_aid_v1          as pkg_aid_v12,
                           pkg_aid_v2          as pkg_aid_v22,

                           from                as from2
                    from and.EVAL_involved_pm_paper) using pm_ao2
;

-- 1890
select count()
from and.GS_dataset_rich_features;

-- drop table and.Synthetic_dataset_rich_features;
create materialized view if not exists and.Synthetic_dataset_rich_features ENGINE = Log populate as
select *
from ( select *, concat(pm_id2, '_', matched_author_order2) as pm_ao2
       from (
                select *, concat(pm_id1, '_', matched_author_order1) as pm_ao1
                from (
                         select *
                         from (
                                  select *,
                                         concat(lastname1, '_', substring(initials1, 1, 1)) as lastname_firat_initial1,
                                         concat(lastname2, '_', substring(initials2, 1, 1)) as lastname_firat_initial2
                                  from (
                                           select *
                                           from and.SYNTHETIC_dataset
                                                    any
                                                    left join (select pm_id                          as pm_id1,
                                                                      journal                        as journal1,
                                                                      article_title                  as article_title1,
                                                                      abstract_str                   as abstract_str1,
                                                                      authors                        as authors1,
                                                                      languages                      as languages1,
                                                                      publication_types              as publication_types1,
                                                                      vernacular_title               as vernacular_title1,
                                                                      suppl_meshs                    as suppl_meshs1,
                                                                      mesh_headings                  as mesh_headings1,
                                                                      other_abstracts                as other_abstracts1,
                                                                      keywords                       as keywords1,
                                                                      references                     as references1,
                                                                      datetime_str                   as datetime_str1,
--                       mag_paper_id as mag_paper_id1,
                                                                      mag_author_id_list_with_orders as mag_author_id_list_with_orders1,
                                                                      s2_author_id_list_with_orders  as s2_author_id_list_with_orders1,
                                                                      mag_citing_author_affiliations as mag_citing_author_affiliations1,
                                                                      mag_citing_paper_info          as mag_citing_paper_info1,
                                                                      mag_cited_paper_infos          as mag_cited_paper_info1,
--                       s2_paper_id as s2_paper_id1,
                                                                      s2_citing_paper_info           as s2_citing_paper_info1,
                                                                      s2_cited_paper_infos           as s2_cited_paper_info1,

                                                                      pkg_aid_v1_coauthors           as pkg_aid_v1_coauthors1,
                                                                      pkg_aid_v2_coauthors           as pkg_aid_v2_coauthors1,
                                                                      pkg_aid_strong_coauthors       as pkg_aid_strong_coauthors1,
                                                                      vetle_aid_coauthors            as vetle_aid_coauthors1,

                                                                      clean_title                    as clean_title1,
                                                                      clean_keywords                 as clean_keywords1,
                                                                      clean_mesh_headings            as clean_mesh_headings1,
                                                                      clean_abstract                 as clean_abstract1
                                                               from and.EVAL_raw_dataset_with_clean_content)
                                                              using pm_id1) any
                                           left join (select pm_id                          as pm_id2,
                                                             journal                        as journal2,
                                                             article_title                  as article_title2,
                                                             abstract_str                   as abstract_str2,
                                                             authors                        as authors2,
                                                             languages                      as languages2,
                                                             publication_types              as publication_types2,
                                                             vernacular_title               as vernacular_title2,
                                                             suppl_meshs                    as suppl_meshs2,
                                                             mesh_headings                  as mesh_headings2,
                                                             other_abstracts                as other_abstracts2,
                                                             keywords                       as keywords2,
                                                             references                     as references2,
                                                             datetime_str                   as datetime_str2,
--                                                                                                          mag_paper_id as mag_paper_id2,
                                                             mag_author_id_list_with_orders as mag_author_id_list_with_orders2,
                                                             s2_author_id_list_with_orders  as s2_author_id_list_with_orders2,
                                                             mag_citing_author_affiliations as mag_citing_author_affiliations2,
                                                             mag_citing_paper_info          as mag_citing_paper_info2,
                                                             mag_cited_paper_infos          as mag_cited_paper_info2,
--                                                                                                          s2_paper_id as s2_paper_id2,
                                                             s2_citing_paper_info           as s2_citing_paper_info2,
                                                             s2_cited_paper_infos           as s2_cited_paper_info2,

                                                             pkg_aid_v1_coauthors           as pkg_aid_v1_coauthors2,
                                                             pkg_aid_v2_coauthors           as pkg_aid_v2_coauthors2,
                                                             pkg_aid_strong_coauthors       as pkg_aid_strong_coauthors2,
                                                             vetle_aid_coauthors            as vetle_aid_coauthors2,

                                                             clean_title                    as clean_title2,
                                                             clean_keywords                 as clean_keywords2,
                                                             clean_mesh_headings            as clean_mesh_headings2,
                                                             clean_abstract                 as clean_abstract2
                                                      from and.EVAL_raw_dataset_with_clean_content) using pm_id2) any
                                  left join (select lastname_firat_initial as lastname_firat_initial1,
                                                    commonness             as name_commonness1
                                             from and.lastname_firat_initial_commonness) using lastname_firat_initial1)
                         any
                         left join (select lastname_firat_initial as lastname_firat_initial2,
                                           commonness             as name_commonness2
                                    from and.lastname_firat_initial_commonness) using lastname_firat_initial2) any
                left join (select pm_ao               as pm_ao1,
                                  one_author          as one_author1,
                                  aminer_paper_id     as aminer_paper_id1,
                                  aminer_author_order as aminer_author_order1,
                                  aminer_author_id    as aminer_author_id1,
                                  s2_paper_id         as s2_paper_id1,
                                  s2_author_order     as s2_author_order1,
                                  s2_author_ids[1]    as s2_author_id1,
                                  mag_paper_id        as mag_paper_id1,
                                  mag_author_order    as mag_author_order1,
                                  mag_author_id       as mag_author_id1,

                                  vetle_aid           as vetle_aid1,
                                  pkg_aid_strong      as pkg_aid_strong1,
                                  pkg_aid_v1          as pkg_aid_v11,
                                  pkg_aid_v2          as pkg_aid_v21,

                                  from                as from1
                           from and.EVAL_involved_pm_paper) using pm_ao1) any
         left join (select pm_ao               as pm_ao2,
                           one_author          as one_author2,
                           aminer_paper_id     as aminer_paper_id2,
                           aminer_author_order as aminer_author_order2,
                           aminer_author_id    as aminer_author_id2,
                           s2_paper_id         as s2_paper_id2,
                           s2_author_order     as s2_author_order2,
                           s2_author_ids[1]    as s2_author_id2,
                           mag_paper_id        as mag_paper_id2,
                           mag_author_order    as mag_author_order2,
                           mag_author_id       as mag_author_id2,

                           vetle_aid           as vetle_aid2,
                           pkg_aid_strong      as pkg_aid_strong2,
                           pkg_aid_v1          as pkg_aid_v12,
                           pkg_aid_v2          as pkg_aid_v22,

                           from                as from2
                    from and.EVAL_involved_pm_paper) using pm_ao2
;

-- 87231
select count()
from and.Synthetic_dataset_rich_features;

-- drop table and.WHU_dataset_rich_features;
create materialized view if not exists and.WHU_dataset_rich_features ENGINE = Log populate as
select *
from ( select *, concat(pm_id2, '_', matched_author_order2) as pm_ao2
       from (
                select *, concat(pm_id1, '_', matched_author_order1) as pm_ao1
                from (
                         select *
                         from (
                                  select *,
                                         concat(lastname1, '_', substring(initials1, 1, 1)) as lastname_firat_initial1,
                                         concat(lastname2, '_', substring(initials2, 1, 1)) as lastname_firat_initial2
                                  from (
                                           select *
                                           from and.WHU_dataset
                                                    any
                                                    left join (select pm_id                          as pm_id1,
                                                                      journal                        as journal1,
                                                                      article_title                  as article_title1,
                                                                      abstract_str                   as abstract_str1,
                                                                      authors                        as authors1,
                                                                      languages                      as languages1,
                                                                      publication_types              as publication_types1,
                                                                      vernacular_title               as vernacular_title1,
                                                                      suppl_meshs                    as suppl_meshs1,
                                                                      mesh_headings                  as mesh_headings1,
                                                                      other_abstracts                as other_abstracts1,
                                                                      keywords                       as keywords1,
                                                                      references                     as references1,
                                                                      datetime_str                   as datetime_str1,
                                                                      mag_author_id_list_with_orders as mag_author_id_list_with_orders1,
                                                                      s2_author_id_list_with_orders  as s2_author_id_list_with_orders1,
                                                                      mag_citing_author_affiliations as mag_citing_author_affiliations1,
                                                                      mag_citing_paper_info          as mag_citing_paper_info1,
                                                                      mag_cited_paper_infos          as mag_cited_paper_info1,
                                                                      s2_citing_paper_info           as s2_citing_paper_info1,
                                                                      s2_cited_paper_infos           as s2_cited_paper_info1,
                                                                      pkg_aid_v1_coauthors           as pkg_aid_v1_coauthors1,
                                                                      pkg_aid_v2_coauthors           as pkg_aid_v2_coauthors1,
                                                                      pkg_aid_strong_coauthors       as pkg_aid_strong_coauthors1,
                                                                      vetle_aid_coauthors            as vetle_aid_coauthors1,
                                                                      clean_title                    as clean_title1,
                                                                      clean_keywords                 as clean_keywords1,
                                                                      clean_mesh_headings            as clean_mesh_headings1,
                                                                      clean_abstract                 as clean_abstract1
                                                               from and.EVAL_raw_dataset_with_clean_content)
                                                              using pm_id1) any
                                           left join (select pm_id                          as pm_id2,
                                                             journal                        as journal2,
                                                             article_title                  as article_title2,
                                                             abstract_str                   as abstract_str2,
                                                             authors                        as authors2,
                                                             languages                      as languages2,
                                                             publication_types              as publication_types2,
                                                             vernacular_title               as vernacular_title2,
                                                             suppl_meshs                    as suppl_meshs2,
                                                             mesh_headings                  as mesh_headings2,
                                                             other_abstracts                as other_abstracts2,
                                                             keywords                       as keywords2,
                                                             references                     as references2,
                                                             datetime_str                   as datetime_str2,
                                                             mag_author_id_list_with_orders as mag_author_id_list_with_orders2,
                                                             s2_author_id_list_with_orders  as s2_author_id_list_with_orders2,
                                                             mag_citing_author_affiliations as mag_citing_author_affiliations2,
                                                             mag_citing_paper_info          as mag_citing_paper_info2,
                                                             mag_cited_paper_infos          as mag_cited_paper_info2,
                                                             s2_citing_paper_info           as s2_citing_paper_info2,
                                                             s2_cited_paper_infos           as s2_cited_paper_info2,
                                                             pkg_aid_v1_coauthors           as pkg_aid_v1_coauthors2,
                                                             pkg_aid_v2_coauthors           as pkg_aid_v2_coauthors2,
                                                             pkg_aid_strong_coauthors       as pkg_aid_strong_coauthors2,
                                                             vetle_aid_coauthors            as vetle_aid_coauthors2,
                                                             clean_title                    as clean_title2,
                                                             clean_keywords                 as clean_keywords2,
                                                             clean_mesh_headings            as clean_mesh_headings2,
                                                             clean_abstract                 as clean_abstract2
                                                      from and.EVAL_raw_dataset_with_clean_content) using pm_id2) any
                                  left join (select lastname_firat_initial as lastname_firat_initial1,
                                                    commonness             as name_commonness1
                                             from and.lastname_firat_initial_commonness) using lastname_firat_initial1)
                         any
                         left join (select lastname_firat_initial as lastname_firat_initial2,
                                           commonness             as name_commonness2
                                    from and.lastname_firat_initial_commonness) using lastname_firat_initial2) any
                left join (select pm_ao               as pm_ao1,
                                  one_author          as one_author1,
                                  aminer_paper_id     as aminer_paper_id1,
                                  aminer_author_order as aminer_author_order1,
                                  aminer_author_id    as aminer_author_id1,
                                  s2_paper_id         as s2_paper_id1,
                                  s2_author_order     as s2_author_order1,
                                  s2_author_ids[1]    as s2_author_id1,
                                  mag_paper_id        as mag_paper_id1,
                                  mag_author_order    as mag_author_order1,
                                  mag_author_id       as mag_author_id1,
                                  vetle_aid           as vetle_aid1,
                                  pkg_aid_strong      as pkg_aid_strong1,
                                  pkg_aid_v1          as pkg_aid_v11,
                                  pkg_aid_v2          as pkg_aid_v21,
                                  from                as from1
                           from and.EVAL_involved_pm_paper) using pm_ao1) any
         left join (select pm_ao               as pm_ao2,
                           one_author          as one_author2,
                           aminer_paper_id     as aminer_paper_id2,
                           aminer_author_order as aminer_author_order2,
                           aminer_author_id    as aminer_author_id2,
                           s2_paper_id         as s2_paper_id2,
                           s2_author_order     as s2_author_order2,
                           s2_author_ids[1]    as s2_author_id2,
                           mag_paper_id        as mag_paper_id2,
                           mag_author_order    as mag_author_order2,
                           mag_author_id       as mag_author_id2,
                           vetle_aid           as vetle_aid2,
                           pkg_aid_strong      as pkg_aid_strong2,
                           pkg_aid_v1          as pkg_aid_v12,
                           pkg_aid_v2          as pkg_aid_v22,
                           from                as from2
                    from and.EVAL_involved_pm_paper) using pm_ao2
;

-- 10
select from1, from2
from and.WHU_dataset_rich_features
where length(from1) > 1
   or length(from2) > 1;

select is_train, count()
from and.WHU_dataset_rich_features
group by is_train;

-- 在数据库中保存模型计算的features
create table if not exists and.GS_features
(
    same_author             Int32,
    same_aminer_author_id   Int32,
    same_s2_author_id       Int32,
    same_mag_author_id      Int32,
    name_commonness         Int32,
    same_lastname           Int32,
    initial_len_score       Int32,
    both_initial_firstname  Int32,
    co_authors_within_paper Int32,
    use_same_language       Int32,
    common_pub_types        Int32,
    non_us_funded           Int32,
    use_vernacular          Int32,
    co_keywords             Int32,
    cross_include_words     Int32,
    common_cited_journal    Int32,
    pub_year_diff           Int32,
    mag_co_author_id        Int32,
    s2_co_author_id         Int32
) ENGINE = Log;

-- 1729
select count()
from and.GS_features;

select avg(same_author),
       avg(same_aminer_author_id),
       avg(same_s2_author_id),
       avg(same_mag_author_id),
       avg(name_commonness),
       avg(same_lastname),
       avg(initial_len_score),
       avg(both_initial_firstname),
       avg(co_authors_within_paper),
       avg(use_same_language),
       avg(common_pub_types),
       avg(non_us_funded),
       avg(use_vernacular),
       avg(co_keywords),
       avg(cross_include_words),
       avg(common_cited_journal),
       avg(pub_year_diff),
       avg(mag_co_author_id),
       avg(s2_co_author_id)
from and.GS_features;

-- 可以使用的features
select same_author,
       aminer_author_id1,
       aminer_author_id2,
       s2_author_id1,
       s2_author_id2,
       mag_author_id1,
       mag_author_id2,
       name_commonness1,
       name_commonness2,
       lastname1,
       lastname2,
       initials1,
       initials2,
       matched_author_order1,
       matched_author_order2,
       journal1,
       journal2,
       authors1,
       authors2,
       languages1,
       languages2,
       publication_types1,
       publication_types2,
       vernacular_title1,
       vernacular_title2,
       article_title1,
       article_title2,
       abstract_str1,
       abstract_str2,
       suppl_meshs1,
       suppl_meshs2,
       mesh_headings1,
       mesh_headings2,
       other_abstracts1,
       other_abstracts2,
       keywords1,
       keywords2,
       clean_title1,
       clean_title2,
       clean_abstract1,
       clean_abstract2,
       clean_keywords1,
       clean_keywords2,
       clean_mesh_headings1,
       clean_mesh_headings2,
       references1,
       references2,
       datetime_str1,
       datetime_str2,
       mag_author_id_list_with_orders1,
       mag_author_id_list_with_orders2,
       s2_author_id_list_with_orders1,
       s2_author_id_list_with_orders2,
       mag_citing_author_affiliations1,
       mag_citing_author_affiliations2,
       mag_citing_paper_info1,
       mag_citing_paper_info2,
       mag_cited_paper_info1,
       mag_cited_paper_info2,
       s2_citing_paper_info1,
       s2_citing_paper_info2,
       s2_cited_paper_info1,
       s2_cited_paper_info2
from and.Synthetic_dataset_rich_features;



select count()
from and.Synthetic_dataset_rich_features;
select same_author,
       is_train,
       pm_ao1,
       pm_ao2,
       author_list1[toUInt32OrZero(matched_author_order1)]                                                      as author_names1,
       author_list2[toUInt32OrZero(matched_author_order2)]                                                      as author_names2,
       lastname1,
       lastname2,
       initials1,
       initials2,
       name_commonness1,
       name_commonness2,
       aminer_author_id1,
       aminer_author_id2,
       mag_author_id1,
       mag_author_id2,
       s2_author_id1,
       s2_author_id2,

       CAST(if(pkg_aid_v21 is null or pkg_aid_v21 == '', '0', pkg_aid_v21),
            'String')                                                                                           as pkg_aid_v21,
       CAST(if(pkg_aid_v22 is null or pkg_aid_v22 == '', '0', pkg_aid_v22),
            'String')                                                                                           as pkg_aid_v22,
       CAST(if(pkg_aid_v11 is null or pkg_aid_v11 == '', '0', pkg_aid_v11),
            'String')                                                                                           as pkg_aid_v11,
       CAST(if(pkg_aid_v12 is null or pkg_aid_v12 == '', '0', pkg_aid_v12),
            'String')                                                                                           as pkg_aid_v12,
       CAST(if(vetle_aid1 is null or vetle_aid1 == '', '0', vetle_aid1),
            'String')                                                                                           as vetle_aid1,
       CAST(if(vetle_aid2 is null or vetle_aid2 == '', '0', vetle_aid2),
            'String')                                                                                           as vetle_aid2,

       languages1,
       languages2,
       publication_types1,
       publication_types2,
       vernacular_title1,
       vernacular_title2,
       vernacular_title1 is not null and length(vernacular_title1) > 0 ? 1 :0                                   as use_vernacular_title1,
       vernacular_title2 is not null and length(vernacular_title2) > 0 ? 1 :0                                   as use_vernacular_title2,
       arrayDistinct(splitByChar(' ', arrayStringConcat([clean_title1, clean_keywords1, clean_mesh_headings1],
                                                        ' ')))                                                  as paper_words1,
       arrayDistinct(splitByChar(' ', arrayStringConcat([clean_title2, clean_keywords2, clean_mesh_headings2],
                                                        ' ')))                                                  as paper_words2,
       references1,
       references2,
       toUInt32OrZero(substring(datetime_str1, 1, 4))                                                           as pub_year1,
       toUInt32OrZero(substring(datetime_str2, 1, 4))                                                           as pub_year2,
       arrayFilter(x-> x != lastname_firat_initial1, arrayDistinct(arrayMap(x->concat(x[2], '_', substring(x[4], 1, 1)),
                                                                            author_list1)))                     as coauthors_within_paper1,
       arrayFilter(x-> x != lastname_firat_initial2, arrayDistinct(arrayMap(x->concat(x[2], '_', substring(x[4], 1, 1)),
                                                                            author_list2)))                     as coauthors_within_paper2,
       arrayFilter(x-> x != toString(mag_author_id1), arrayDistinct(arrayMap(x->x[2],
                                                                             mag_author_id_list_with_orders1))) as mag_coauthor_ids1,
       arrayFilter(x-> x != toString(mag_author_id2), arrayDistinct(arrayMap(x->x[2],
                                                                             mag_author_id_list_with_orders2))) as mag_coauthor_ids2,
       arrayFilter(x-> x != s2_author_id1, arrayDistinct(arrayMap(x->x[2],
                                                                  s2_author_id_list_with_orders1)))             as s2_coauthor_ids1,
       arrayFilter(x-> x != s2_author_id2, arrayDistinct(arrayMap(x->x[2],
                                                                  s2_author_id_list_with_orders2)))             as s2_coauthor_ids2,

       arrayFilter(x-> x != pkg_aid_v21,
                   arrayDistinct(pkg_aid_v2_coauthors1))                                                        as pkg_coauthor_ids1,
       arrayFilter(x-> x != pkg_aid_v22,
                   arrayDistinct(pkg_aid_v2_coauthors2))                                                        as pkg_coauthor_ids2,
       arrayFilter(x-> x != pkg_aid_v11,
                   arrayDistinct(pkg_aid_v1_coauthors1))                                                        as pkg_v1_coauthor_ids1,
       arrayFilter(x-> x != pkg_aid_v12,
                   arrayDistinct(pkg_aid_v1_coauthors2))                                                        as pkg_v1_coauthor_ids2,
       arrayFilter(x-> x != vetle_aid1,
                   arrayDistinct(vetle_aid_coauthors1))                                                         as vetle_coauthor_ids1,
       arrayFilter(x-> x != vetle_aid2,
                   arrayDistinct(vetle_aid_coauthors2))                                                         as vetle_coauthor_ids2
--        from and.Synthetic_dataset_rich_features;
-- from and.SONG_dataset_rich_features;
-- from and.SONG_dataset_balanced_rich_features;
-- from and.GS_dataset_rich_features;
from and.WHU_dataset_rich_features;


SELECT id,
       pm_ao,
       one_author                                                                  as author_names,
       lastname_firat_initial                                                      as ns,
       commonness,
       fn_max_len_str,
       fn_contain_dash,
       fn_contain_other_chars,
       pub_year,
       datetime_str,
       aminer_author_id,
       mag_author_id,
       s2_author_ids[1]                                                            as s2_author_id,

       CAST(if(pkg_aid_v2 is null or pkg_aid_v2 == '', '0', pkg_aid_v2), 'String') as pkg_aid_v2,
       CAST(if(pkg_aid_v1 is null or pkg_aid_v1 == '', '0', pkg_aid_v1), 'String') as pkg_aid_v1,
       CAST(if(vetle_aid is null or vetle_aid == '', '0', vetle_aid), 'String')    as vetle_aid,

       arrayFilter(x-> x != lastname_firat_initial, co_author_name)                as co_author_name,
       arrayFilter(x-> x != mag_author_id, mag_co_author_id)                       as mag_co_author_id,
       arrayFilter(x-> x != s2_author_id, s2_co_author_id)                         as s2_co_author_id,
       arrayFilter(x-> x != pkg_aid_v2, pkg_co_author_id)                          as pkg_co_author_id,
       arrayFilter(x-> x != pkg_aid_v1, pkg_v1_co_author_id)                       as pkg_v1_co_author_id,
       arrayFilter(x-> x != vetle_aid, vetle_co_author_id)                         as vetle_co_author_id,
       arrayDistinct(splitByChar(' ', arrayStringConcat([clean_title, clean_keywords, clean_mesh_headings],
                                                        ' ')))                     as paper_words
from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn;


--失效的作者ID的数量
-- 25412816
select count()
from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn
where mag_author_id == 0;
-- 1756841
select count()
from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn
where s2_author_ids[1] is null
   or s2_author_ids[1] == '';
-- 8526012
select count()
from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn
where pkg_aid_v2 is null
   or pkg_aid_v2 == ''
   or pkg_aid_v2 == '0';
-- 8244169
select count()
from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn
where pkg_aid_v1 is null
   or pkg_aid_v1 == ''
   or pkg_aid_v1 == '0';
-- 60476723
select count()
from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn
where vetle_aid is null
   or vetle_aid == ''
   or vetle_aid == '0';


-- drop table and.Synthetic_dataset_normal_features_for_model_verification;
create materialized view if not exists and.Synthetic_dataset_normal_features_for_model_verification ENGINE = Log populate as
select *
from (
         SELECT id,
                pm_ao,
                one_author                                                                  as author_names,
                lastname_firat_initial                                                      as ns,
                commonness,
                fn_max_len_str,
                fn_contain_dash,
                fn_contain_other_chars,
                pub_year,
                datetime_str,
                aminer_author_id,
                mag_author_id,
                s2_author_ids[1]                                                            as s2_author_id,

                CAST(if(pkg_aid_v2 is null or pkg_aid_v2 == '', '0', pkg_aid_v2), 'String') as pkg_aid_v2,
                CAST(if(pkg_aid_v1 is null or pkg_aid_v1 == '', '0', pkg_aid_v1), 'String') as pkg_aid_v1,
--                 CAST(if(pkg_aid_strong is null or pkg_aid_strong == '', '0', pkg_aid_strong), 'String') as pkg_aid_strong,
                CAST(if(vetle_aid is null or vetle_aid == '', '0', vetle_aid), 'String')    as vetle_aid,

                arrayFilter(x-> x != lastname_firat_initial, co_author_name)                as co_author_name,
                arrayFilter(x-> x != mag_author_id, mag_co_author_id)                       as mag_co_author_id,
                arrayFilter(x-> x != s2_author_id, s2_co_author_id)                         as s2_co_author_id,
                arrayFilter(x-> x != pkg_aid_v2, pkg_co_author_id)                          as pkg_co_author_id,
                arrayFilter(x-> x != pkg_aid_v1, pkg_v1_co_author_id)                       as pkg_v1_co_author_id,
--                 arrayFilter(x-> x != pkg_aid_strong, pkg_strong_co_author_id) as pkg_strong_co_author_id,
                arrayFilter(x-> x != vetle_aid, vetle_co_author_id)                         as vetle_co_author_id,
                arrayDistinct(splitByChar(' ', arrayStringConcat([clean_title, clean_keywords, clean_mesh_headings],
                                                                 ' ')))                     as paper_words
         from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn) any
         inner join (select arrayJoin(
                                    arrayDistinct(arrayConcat(groupUniqArray(pm_ao1), groupUniqArray(pm_ao2)))) as pm_ao
                     from and.Synthetic_dataset_rich_features) using pm_ao;


-- select CAST(if(pkg_aid_v2 is null or pkg_aid_v2 == '', '0', pkg_aid_v2), 'String')             as pkg_aid_v2,
--        CAST(if(pkg_aid_v1 is null or pkg_aid_v1 == '', '0', pkg_aid_v1), 'String')             as pkg_aid_v1,
--        CAST(if(pkg_aid_strong is null or pkg_aid_strong == '', '0', pkg_aid_strong), 'String') as pkg_aid_strong,
--        CAST(if(vetle_aid is null or vetle_aid == '', '0', vetle_aid), 'String')                as vetle_aid,
--
--        arrayFilter(x-> x != lastname_firat_initial, co_author_name)                            as co_author_name,
--        arrayFilter(x-> x != mag_author_id, mag_co_author_id)                                   as mag_co_author_id,
--        arrayFilter(x-> x != s2_author_ids[1], s2_co_author_id)                                 as s2_co_author_id,
--        arrayFilter(x-> x != pkg_aid_v2, pkg_co_author_id)                                      as pkg_co_author_id,
--        arrayFilter(x-> x != pkg_aid_v1, pkg_v1_co_author_id)                                   as pkg_v1_co_author_id,
--        arrayFilter(x-> x != vetle_aid, vetle_co_author_id)                                     as vetle_co_author_id
-- from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn
-- limit 100000, 500;

select arrayJoin(arrayConcat(groupUniqArray(pm_ao1), groupUniqArray(pm_ao2))) as pm_ao
from and.Synthetic_dataset_rich_features;

select length(arrayDistinct(arrayConcat(groupUniqArray(pm_ao1), groupUniqArray(pm_ao2)))) as pm_ao
from and.Synthetic_dataset_rich_features;
select length(arrayConcat(groupUniqArray(pm_ao1), groupUniqArray(pm_ao2))) as pm_ao
from and.Synthetic_dataset_rich_features;
-- 6306
select count()
from and.Synthetic_dataset_normal_features_for_model_verification;
select count()
from and.Synthetic_dataset_rich_features;
select count()
from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn;

