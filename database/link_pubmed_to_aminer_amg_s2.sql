-- 从pubmed中找到pm_id与pmc_id的对应关系
-- 2287563/2433881, 大概之后146318个pmc文章没有对应的pmid
-- drop table pubmed.pmc_pm_id_mapping;
create materialized view if not exists and.pmc_pm_id_mapping ENGINE = Log populate as
select pmc_id,
       if(length(trimBoth(JSONExtractString(paper_ids, 'pmid')) as pm_id_str) > 0, toUInt64OrZero(pm_id_str),
          pm_id) as pm_id
from pubmed.paper
where pm_id != 0;

select pm_id  from pubmed.nft_paper;

-- 2287563
select count()
from and.pmc_pm_id_mapping;

-----------------------------------------------------------------------------------------------------------------------------


-- 统计 MAG 中的数据导入情况
-- root@node:~/data1/mag# wc -l PaperAuthorAffiliations.txt
-- 566136708 PaperAuthorAffiliations.txt
-- 566134841
select count()
from mag.paper_author_affiliation;
-- truncate table mag.paper_author_affiliation;


-- 从 MAG中提取pubmed论文
-----------------------------------------------------------------------------------------------------------------------------
-- 从下面的几个url ('ncbi.nlm.nih.gov', 'europepmc.org', 'pubmed.cn') 中抽取pubmed
-- pubmed 的文章主要来自于这几个网站('ncbi.nlm.nih.gov', 'europepmc.org', 'pubmed.cn')
select domainWithoutWWW(lower(SourceUrl) as sourceurl) as domain, count() as cnt
from mag.paper_urls
where position(sourceurl, 'med') > 0
   or position(sourceurl, 'pubmed') > 0
   or position(sourceurl, 'pmc') > 0
group by domain
order by cnt desc
limit 500;

select SourceUrl
from mag.paper_urls
where domainWithoutWWW(lower(SourceUrl)) in ('ncbi.nlm.nih.gov', 'europepmc.org', 'pubmed.cn')
limit 100;

-- 29489808	24609188	24647985
select count() as cnt, count(distinct PaperId), count(distinct pm_id) as pmids
from and.mag_pm_id_candidate;

-- drop table mag.mag_pm_id_candidate;
create materialized view if not exists and.mag_pm_id_candidate ENGINE = Log populate as
select PaperId, if(length(t1.pm_id) = 0, toString(t2.pm_id), t1.pm_id) as pm_id, pmc_id, sourceurl
from (
         select PaperId,
                lower(SourceUrl)                                as sourceurl,
                if(position(sourceurl, 'med/') > 0, extract(extract(sourceurl, 'med/\\d+'), '\\d+'),
                   if(position(sourceurl, 'pubmed.cn/') > 0, extract(sourceurl, '\\d+'),
                      ''))                                      as pm_id,
                extract(extract(sourceurl, '/pmc\\d+'), '\\d+') as pmc_id
         from mag.paper_urls
         where domainWithoutWWW(sourceurl) in ('ncbi.nlm.nih.gov', 'europepmc.org', 'pubmed.cn')
         ) as t1 any
         left join and.pmc_pm_id_mapping as t2 on t1.pmc_id = toString(t2.pmc_id)
where pm_id != '0';

-- 4	37
-- 3	503
-- 2	41225
-- 1	24567423
-- 抽取出来的pubmed确实存在同一个paperID对应多个pubmedid的情况
select len, count() as cnt
from (
      select PaperId, groupUniqArray(pm_id) as pmids, length(pmids) as len, groupUniqArray(pmc_id) as pmcids
      from and.mag_pm_id_candidate
      group by PaperId
      order by len
          desc)
group by len;

-- 3	6
-- 2	3533
-- 1	24644446
select len, count() as cnt
from (
      select pm_id, groupUniqArray(PaperId) as PaperIds, length(PaperIds) as len, groupUniqArray(pmc_id) as pmcids
      from and.mag_pm_id_candidate
      group by pm_id
      order by len
          desc)
group by len;

-- 29489808	24647985	24651530 mag 中有数据集的重复问题
select count(), count(distinct pm_id), count(distinct concat(toString(pm_id), '_', toString(PaperId)))
from and.mag_pm_id_candidate;
-----------------------------------------------------------------------------------------------------------------------------


-- 发现 mag_pm_id_candidate 里面有重复的，也就是说mag paper id 和 pm_id 不是一对一的关系，TODO 这里忽略一个pmid 对应多个 paperID的情况
-- drop table mag.mag_pm_id;
create materialized view if not exists and.mag_pm_id ENGINE = Log populate as
select PaperId, pm_id, pmc_id
from and.mag_pm_id_candidate any
         inner join(
    select pm_id
    from and.mag_pm_id_candidate
    group by pm_id
             -- Bug fix, 此处将groupArray修改成groupUniqArray，能够改变数据集的大小，因为已经验证了 pmid + paperid有重合的情况
    having length(groupUniqArray(PaperId)) = 1) using pm_id;

-- MAG 与 pubmed 的文章对应关系
-- 29479378	24644446	24602298
select count(), count(distinct pm_id), count(distinct PaperId)
from and.mag_pm_id;

-- create materialized view if not exists mag.mag_pm_id ENGINE = Log populate as
-- select PaperId, if(length(pm_id) > 0, pm_id, mapping_pm_id) as pm_id, pmc_id
select PaperId, pm_id, mapping_pm_id, pmc_id
from (select * from and.mag_pm_id_candidate where length(pmc_id) > 0) any
         left join (select toString(pmc_id) as pmc_id, toString(pm_id) as mapping_pm_id from and.pmc_pm_id_mapping)
                   using pmc_id
order by MD5(pmc_id)
limit 500;


-- 29489808
select count()
from and.mag_pm_id_candidate;

--使用MAG的作者的ID给pubmed增添作者
-- drop table mag.mag_pm_id_author_id;
create materialized view if not exists and.mag_pm_id_author_id ENGINE = Log populate as
select PaperId,
       AuthorId,
       AffiliationId,
       AuthorSequenceNumber,
       OriginalAuthor,
       OriginalAffiliation,
       pm_id,
       pmc_id
from mag.paper_author_affiliation as t1 any
         inner join and.mag_pm_id as t2
                    on toUInt64(t1.PaperId) = t2.PaperId
order by pm_id, AuthorSequenceNumber;

-- 1545 of 98146543 只有1545个pmid带有前缀‘0’  可以忽略不计
select count() from and.mag_pm_id_author_id where startsWith(pm_id, '0');

select count()
from pubmed.nft_paper;

--  通过mag关联到的pubmed的authorid
-- 98146543	98146543	24602297	98146543	24602297	3.9893
select count()                        as cnt,
       count(PaperId)                 as pids,
       count(distinct PaperId)        as distinct_pids,
       count(pm_id)                   as pmids,
       count(distinct pm_id)          as distinct_pmids,
       round(cnt / distinct_pmids, 4) as avg_author_count
from and.mag_pm_id_author_id;

-- 从 mag 中计算出来的pubmed论文平均作者个数偏少，3.9893<4.02
-- 这是因为 mag.paper_author_affiliation 自身的问题，我们统计了做这个个数>8的论文数量，发现 mag 最少
select count() as cnt from (
select count() as au_cnt from and.aminer_pm_id_author_id group by pm_id) -- 1939528
-- select count() as au_cnt from semantic_scholar.s2_pm_id_author_id group by pm_id) -- 2341648
-- select count() as au_cnt from mag.mag_pm_id_author_id group by pm_id) -- 1809081
where au_cnt>8;
------------------------------------------------------------------------------------------------------------------------------------------------

-- 27828008 该步骤的目的是从aminer_paper的url字段收集pubmed id，从结果上看验证了Aminer中基本包含了pubmed
select count()
from aminer.paper
where length(arrayFilter(x -> position(x, 'pubmed') > 0, url)) > 0;

-- 验证 aminer 中识别来自pubmed的link, 主要来自于下面这个domain 'ncbi.nlm.nih.gov'
-- ncbi.nlm.nih.gov	27827567
-- dx.doi.org	7898
-- doi.org	236
select domain, count() as cnt
from (
         select arrayDistinct(
                        arrayMap(x -> domainWithoutWWW(x),
                                 arrayFilter(x -> position(x, 'pubmed') > 0 or position(x, 'pmc') > 0, url)
                            )
                    ) as pm_source_domains
         from aminer.paper
         where length(pm_source_domains) > 0)
         array join pm_source_domains as domain
group by domain
order by cnt desc;

-- 验证 正则表达式提取的正确性
select arrayFilter(x -> domainWithoutWWW(x) == 'ncbi.nlm.nih.gov', url)                       as u,
       arrayFilter(x -> length(x) > 0, arrayMap(x -> extract(x, '\\d+'),
                                                arrayMap(x -> extract(x, 'pubmed/\\d+'), u))) as pm_ids
from aminer.paper
where length(u) > 0
limit 500;

-- drop table aminer.aminer_pm_id_condidate;
create materialized view if not exists and.aminer_pm_id_condidate ENGINE = Log populate as
select id,
       arrayMap(x-> JSONExtractString(x, 'name'), extractAll(authors, '\\{[^\\}]+\\}')) as author_name_list,
       title,
       keywords,
       venue,
       year,
       arrayFilter(x -> length(x) > 0, arrayMap(x -> extract(x, '\\d+'),
                                                arrayMap(x -> extract(x, 'pubmed/\\d+'),
                                                         arrayFilter(x -> domainWithoutWWW(x) == 'ncbi.nlm.nih.gov',
                                                                     url))))            as pm_ids
from aminer.paper
-- 这里只使用了pbumed进行查找，验证了europe和pmc等词并不能从aminer中找到pubmed文章
where length(pm_ids) > 0;

select id,
       authors,
       arrayMap(x-> JSONExtractString(x, 'name'), extractAll(authors, '\\{[^\\}]+\\}')) as author_name_list
from aminer.paper
limit 200;

-- 27828008
select count()
from and.aminer_pm_id_condidate;

-- 201117的有歧义的aminer paper <---> pubmed paper 需要对他们进行消歧
select count()
from and.aminer_pm_id_condidate
where length(pm_ids) > 1;

select arrayElement(pm_ids, 1) as pm_id
from and.aminer_pm_id_condidate
where length(pm_ids) = 1;


select count()
from aminer.author;


-- drop table aminer.aminer_pm_id_author_id;
create materialized view if not exists and.aminer_pm_id_author_id ENGINE = Log populate as
select pm_id, paper_id, author_order, aid
from (
         select id                       as aid,
                tupleElement(pub, 1)     as paper_id,
                tupleElement(pub, 2) + 1 as author_order -- start from 1
         from aminer.author
                  array join arrayMap(x-> JSONExtract(x, 'Tuple(i String, r Float32)'),
                                      extractAll(pubs, '\\{[^\\}]+\\}')) as pub) any   -- 由于 aminer.author 自身的问题，没有包含部分pubmed论文中的作者
         inner join (select id as paper_id, pm_ids[1] as pm_id
                     from and.aminer_pm_id_condidate
                          -- 只有 199304 条记录的 length(pm_ids) != 1 只使用aminer中 pmid 和 aminer id一一对应的论文
                          -- 已经验证了 paper_id 在右边表 aminer_pm_id_condidate 中是唯一的
                     where length(pm_ids) = 1) using paper_id
order by pm_id, author_order;

-- 279364249 279289613 aminer.author 完全展开之后，会出现 paper_id + author_id 的重复现象，但是非常少
select count() as cnt,
       count(distinct concat(tupleElement(pub, 1),'_', id)) as cnt1
from aminer.author
         array join arrayMap(x-> JSONExtract(x, 'Tuple(i String, r Float32)'),
                             extractAll(pubs, '\\{[^\\}]+\\}')) as pub;


-- 279364249 279364249 验证了 extract + 正则表达式提取信息的有效性
select sum(length(extractAll(pubs, '\\{[^\\}]+\\}'))) as extract_sum, sum(length(extractAll(pubs, 'r')))
from aminer.author;

-- 235040/27347440(inner join)
-- 验证 aminer.paper 中的authors字段中作者是按照原始author order存储的
select author_name_list, author_list
from (
         select pm_ids[1] as pm_id, author_name_list, length(author_name_list) as al
         from and.aminer_pm_id_condidate
         where length(pm_ids) = 1) any
         inner join and.nft_paper_author_name_list using pm_id
where length(author_name_list) == length(author_list)
limit 200;

-- 98345795	98345795	24523187	98345795	24522757	4.01
select count() as cnt, count(distinct paper_id) as pids, count(distinct pm_id) as pmids, pmids / cnt
from and.aminer_pm_id_author_id;
select count()                        as cnt,
       count(paper_id)                as pids,
       count(distinct paper_id)       as distinct_pids,
       count(pm_id)                   as pmids,
       count(distinct pm_id)          as distinct_pmids,
       round(cnt / distinct_pmids, 4) as avg_author_count
from and.aminer_pm_id_author_id;


create materialized view if not exists and.s2_pm_id_author_id ENGINE = Log populate as
select paper_id,
       pm_id,
       tupleElement(author, 1)                  as author_order,
       tupleElement(tupleElement(author, 2), 1) as author_name,
       tupleElement(tupleElement(author, 2), 2) as aids
from (
         select id                                                   as paper_id,
                pmid                                                 as pm_id,
                arrayMap(x-> JSONExtract(x, 'Tuple(name String, ids Array(String))'),
                         extractAll(authors, '\\{[^\\}]+\\}'))       as tmp,
                arrayMap(x -> tuple(x, tmp[x]), arrayEnumerate(tmp)) as author_list
         from s2.semantic_scholar
         where length(pmid) > 0)
         array join author_list as author
order by pm_id, author_order;

create materialized view if not exists and.s2_pm_id ENGINE = Log populate as
select id as paper_id,
       pmid
from s2.semantic_scholar
where length(pmid) > 0;

select count()
from and.s2_pm_id;

-- 121838090	121838090	29882668	121838090	29871290	4.0788
select count()                        as cnt,
       count(paper_id)                as pids,
       count(distinct paper_id)       as distinct_pids,
       count(pm_id)                   as pmids,
       count(distinct pm_id)          as distinct_pmids,
       round(cnt / distinct_pmids, 4) as avg_author_count
from and.s2_pm_id_author_id;

-- drop table pubmed.nft_paper_author_name_list;
create materialized view and.nft_paper_author_name_list ENGINE = Log populate as
select toString(pm_id)                            as pm_id,
       article_title,
       datetime_str,
       extractAll(JSONExtractRaw(CAST(authors, 'String'), 'authorList'),
                  '\{[^\}]+\}')                   as authors_list_raw,
       arrayMap(x->
                    array(toString(x),
                          JSONExtractString(authors_list_raw[x], 'lastName'),
                          JSONExtractString(authors_list_raw[x], 'foreName'),
                          JSONExtractString(authors_list_raw[x], 'initials')),
                arrayEnumerate(authors_list_raw)) as author_list
from (select pm_id, article_title, datetime_str, authors from pubmed.nft_paper)
-- 过滤掉作者个数特别多的文章，因为会使得内容爆满
where length(authors_list_raw) < 50;


-- 没有作者的论文有643057
select count()
from and.nft_paper_author_name_list where length(author_list)=0;

-- 统计pubmed数据库中的信息
select count()                        as cnt,
--        count(pm_id)                   as pids,
--        count(distinct pm_id)          as distinct_pids,
       count(pm_id)                   as pmids,
       count(distinct pm_id)          as distinct_pmids,
       round(cnt / distinct_pmids, 4) as avg_author_count
from (select pm_id, author_order
      from pubmed.nft_paper  -- 这里可以使用 range函数替换
               array join arrayEnumerate(arrayWithConstant(JSONLength(JSONExtractRaw(CAST(authors, 'String'), 'authorList')), 1)) as author_order);



-- create materialized view pubmed.nft_paper_author_name_list_part2 ENGINE = Log populate as
-- select toString(pm_id)                            as pm_id,
--        article_title,
--        datetime_str,
--        extractAll(JSONExtractRaw(CAST(authors, 'String'), 'authorList'),
--                   '\{[^\}]+\}')                as authors_list_raw,
--        arrayMap(x->
--                     array(toString(x),
--                           JSONExtractString(authors_list_raw[x], 'lastName'),
--                           JSONExtractString(authors_list_raw[x], 'foreName'),
--                           JSONExtractString(authors_list_raw[x], 'initials')),
--                 arrayEnumerate(authors_list_raw)) as author_list
-- -- from (select pm_id, article_title, datetime_str, authors from pubmed.nft_paper where pm_id>=15000000);
--
-- -- drop table pubmed.nft_paper_author_name_list_part2;  --TODO 数据变少了！
-- create view pubmed.nft_paper_author_name_list as
--     select * from pubmed.nft_paper_author_name_list_part1 union all
--     select * from pubmed.nft_paper_author_name_list_part2;


select count()
from pubmed.nft_paper;
-- -- 14690035
-- select count() from pubmed.nft_paper_author_name_list_part1;
-- --  9977426
-- select count() from pubmed.nft_paper_author_name_list_part2;
select count()
from and.nft_paper_author_name_list;


-- 合并来自aminer 和 semantic scholar 和 mag 的 paper id
-- 由于三个一块同时join需要很大的内存，因此这里分开join
-- 1. 首先拿pubmed与aminer进行join
-- drop table pubmed.pm_aminer_paper_mapping;
create materialized view and.pm_aminer_paper_mapping ENGINE = Log populate as
select *
from (
         select concat(pm_id, '_', pm_author_order) as pm_ao,
                pm_id,
                datetime_str,
                one_author,
                toString(one_author[1])             as pm_author_order
         from and.nft_paper_author_name_list
                  array join author_list as one_author)
         any
         left join (select concat(pm_id, '_', aminer_author_order) as pm_ao,
                           paper_id                                as aminer_paper_id,
                           toString(author_order)                  as aminer_author_order,
                           aid                                     as aminer_author_id
                    from and.aminer_pm_id_author_id)
                   using pm_ao;

select count()
from and.pm_aminer_paper_mapping;

-- 2. 其次，拿pubmed与 s2 join
-- drop table pubmed.pm_s2_paper_mapping;
create materialized view and.pm_s2_paper_mapping ENGINE = Log populate as
select *
from (
         select concat(pm_id, '_', pm_author_order) as pm_ao,
                pm_id,
                datetime_str,
                one_author,
                toString(one_author[1])             as pm_author_order
         from and.nft_paper_author_name_list
                  array join author_list as one_author)
         any
         left join (select concat(pm_id, '_', s2_author_order) as pm_ao,
                           paper_id                            as s2_paper_id,
                           toString(author_order)              as s2_author_order,
                           author_name                         as s2_author_name,
                           aids                                as s2_author_ids
                    from and.s2_pm_id_author_id)
                   using pm_ao;

-- 3.其次，拿pubmed与 mag join
-- drop table pubmed.pm_mag_paper_mapping;
create materialized view and.pm_mag_paper_mapping ENGINE = Log populate as
select *
from (
         select concat(pm_id, '_', pm_author_order) as pm_ao,
                pm_id,
                datetime_str,
                one_author,
                toString(one_author[1])             as pm_author_order
         from and.nft_paper_author_name_list
                  array join author_list as one_author)
         any
         left join (select concat(pm_id, '_', mag_author_order) as pm_ao,
                           PaperId                              as mag_paper_id,
                           toString(AuthorSequenceNumber)       as mag_author_order,
                           OriginalAuthor                       as mag_author_name,
                           AuthorId                             as mag_author_id
                    from and.mag_pm_id_author_id)
                   using pm_ao;

-- 119781963	29766414	24564497
select count(), count(distinct pm_id) as pmids, count(distinct mag_paper_id)
from and.pm_mag_paper_mapping;
-- 119781963	29766414	24317876
select count(), count(distinct pm_id) as pmids, count(distinct aminer_paper_id)
from and.pm_aminer_paper_mapping;
-- 119781963	29766414	29490560 semantic scholar中包含的pubmed文章还是非常全的
select count(), count(distinct pm_id) as pmids, count(distinct s2_paper_id)
from and.pm_s2_paper_mapping;

-- drop table pubmed.pm_aminer_s2_paper_mapping;
create materialized view and.pm_aminer_s2_paper_mapping ENGINE = Log populate as
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
       s2_author_ids
from and.pm_aminer_paper_mapping any
         inner join (select pm_ao, s2_paper_id, s2_author_order, s2_author_name, s2_author_ids
                     from and.pm_s2_paper_mapping) using pm_ao;

-- drop table pubmed.pm_aminer_s2_mag_paper_mapping;
create materialized view and.pm_aminer_s2_mag_paper_mapping ENGINE = Log populate as
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
       mag_author_id
from and.pm_aminer_s2_paper_mapping
         any
         inner join (select pm_ao, mag_paper_id, mag_author_order, mag_author_name, mag_author_id
                     from and.pm_mag_paper_mapping) using pm_ao;

-- 119781963
select count() from and.pm_aminer_s2_mag_paper_mapping;

-- drop table pubmed.pm_aminer_s2_mag_paper_mapping;
-- 能够join三张上亿的表，需要较大的内存，32G内存不够用
-- create materialized view pubmed.pm_aminer_s2_mag_paper_mapping ENGINE = Log populate as
-- select *
-- from (
--          select *
--          from (
--                   select *
--                   from (
--                            select concat(pm_id, '_', pm_author_order) as pm_ao,
--                                   pm_id,
--                                   datetime_str,
--                                   one_author,
--                                   toString(one_author[1])             as pm_author_order
--                            from pubmed.nft_paper_author_name_list
--                                     array join author_list as one_author)
--                            any
--                            left join (select concat(pm_id, '_', aminer_author_order) as pm_ao,
--                                              paper_id                                as aminer_paper_id,
--                                              toString(author_order)                  as aminer_author_order,
--                                              aid                                     as aminer_author_id
--                                       from aminer.aminer_pm_id_author_id)
--                                      using pm_ao) any
--                   left join (select concat(pm_id, '_', s2_author_order) as pm_ao,
--                                     paper_id                            as s2_paper_id,
--                                     toString(author_order)              as s2_author_order,
--                                     author_name                         as s2_author_name,
--                                     aids                                as s2_author_ids
--                              from semantic_scholar.s2_pm_id_author_id)
--                             using pm_ao) any
--          left join (select concat(pm_id, '_', mag_author_order) as pm_ao,
--                            PaperId                              as mag_paper_id,
--                            toString(AuthorSequenceNumber)       as mag_author_order,
--                            OriginalAuthor                       as mag_author_name,
--                            AuthorId                             as mag_author_id
--                     from mag.mag_pm_id_author_id)
--                    using pm_ao;


-- join by pm_id 而不是 pm_id + author_order
-- select *
-- from (
--          select pm_id,
--                 article_title,
--                 datetime_str,
--                 author_list,
--                 author_orders,
--                 aminer_author_count
--                     any
--              inner join (
--         select toString(pm_id)                                                          as pm_id,
--                arraySort(x-> tupleElement(x, 1),name
--                          groupArray(tuple(cast(author_order, 'Int32'), aid, paper_id))) as author_orders,
--                count()                                                                  as aminer_author_count
--         from aminer.aminer_pm_id_author_id
--         group by pm_id
--         having aminer_author_count > 0) using pm_id
--          where length(author_list) = aminer_author_count)
--          any
--          left join
--      (select pm_id,
--              arraySort(x-> tupleElement(x, 1),
--                        groupArray(tuple(cast(author_order, 'Int32'), aid, paper_id))) as author_orders,
--              paper_id,
--              author_order,
--              author_name,
--              aids
--       from semantic_scholar.s2_pm_id_author_id
--       group by pm_id) using pm_id;

-- 统计 Aminer S2 mag 与pubmed 之间的paper link
-- 27628215 199304
select count() from and.aminer_pm_id_condidate where length(pm_ids) = 1;
-- 30453745 0
select count() from and.s2_pm_id where length(pmid) = 0;

-- 24609188 41765
select count() from (
select PaperId, count() as num_pm_ids, length(groupUniqArray(pm_id)) as num_unique_pm_ids from and.mag_pm_id_candidate group by PaperId)
where num_unique_pm_ids>1
-- where num_pm_ids > 1 and num_unique_pm_ids=1
;

-- 29489808	24647985	24651530 mag 中有数据集的重复问题
select count(), count(distinct pm_id), count(distinct concat(toString(pm_id), '_', toString(PaperId)))
from and.mag_pm_id_candidate;

-- pubmed中大约只有39%的作者有affiliation
-- 2378043	6101739
select sum(authors_with_has_aff_cnt) as with_aff_cnt,
       sum(author_cnt)               as all_cnt,
       with_aff_cnt / all_cnt        as pctg
from (
      select arrayMap(x-> JSONHas(authors_list_raw[x], 'aff'),
                      arrayEnumerate(extractAll(JSONExtractRaw(CAST(authors, 'String'), 'authorList'),
                                                '\{[^\}]+\}') as authors_list_raw)) as has_aff_arr,
             arrayCount(x ->x = 1, has_aff_arr)                                     as authors_with_has_aff_cnt,
             length(has_aff_arr)                                                    as author_cnt
      from pubmed.nft_paper
      where rand() % 100 < 5
         );

