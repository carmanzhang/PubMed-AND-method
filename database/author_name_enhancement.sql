-- drop table and.first_name_variations; 查看作者名字pair的各种变异情况
create materialized view if not exists and.first_name_variations ENGINE = Log populate as
select wanted_author_pair[1] as fn1, wanted_author_pair[2] as fn2
from (
      select distinct(arrayReverseSort(x->length(x), firstname_pair)) as wanted_author_pair
      from (
               select lastname_first_initial,
                      min(commonness)          as tmp_commonness,
                      arrayMap(x->
                                   [concat((x.1)[4], ', ', (x.1)[5]), concat((x.2)[4], ', ', (x.2)[5])], arrayFilter(
                                       x->concat((x.1)[4], ', ', (x.1)[5]) != concat((x.2)[4], ', ', (x.2)[5]) and
                                          length(concat((x.1)[4], ', ', (x.1)[5])) > 2 and
                                          length(concat((x.2)[4], ', ', (x.2)[5])) > 2,
                                       arrayMap(x -> (authors[toUInt32(rand() % 10 * 1.0 * ns_len / 10) as seed],
                                                      authors[toUInt32(rand(seed) % 10 * 1.0 * ns_len / 10)]),
                                                range(toUInt32(
                                                        (length(groupArray(paper_author_name) as authors) as ns_len)))
                                           ))) as diff_firstname_pairs
               from and.nft_paper_author_name_in_one_string
               where commonness > 100
--                  and commonness%10 < 3
               group by lastname_first_initial
               order by tmp_commonness desc)
               array join diff_firstname_pairs as firstname_pair);

-- 测试数据集中的姓名缩写情况将会导致AND的效果比较低。作为AND的第一个阶段，我们首先应该对缩写的姓名进行补齐，可以使用MAG和S2对其进行补齐
-- 测试一下 MAG和S2中的姓名完整性，发现MAG和S2中的姓名都是比较完整的，对PubMed是一个很好的补充，解决了pubmed中姓名缩写的情况
-- 72564444
select lower(mag_author_name) as mag_author_name, lower(s2_author_name) as s2_author_name, one_author
from and.pm_aminer_s2_mag_paper_mapping
where length(replaceAll(mag_author_name, '.', '')) > length(s2_author_name) + 5;


-- 在对姓名的缩写还原的同时，还需要对非ascii字符进行转换，下面打算使用外置的脚本对，应该统计并导出姓名中的特殊字符，拿到特殊字符，验证特殊字符-> ascii转化器的正确性
select lower(mag_author_name)                                 as mag_author_name,
       replaceRegexpAll(mag_author_name, '[a-z\-\\s\\.]', '') as special_char
from and.pm_aminer_s2_mag_paper_mapping
where length(special_char) > 0
limit 500;


-- 已经验证了外置脚本对姓名转换的正确性，下面创建表，以存储外置脚本对名字中的特殊字符进行转换的结果
select count(), toUInt32OrZero(substring(pm_ao, 2, 1)) as x
from and.pm_aminer_s2_mag_paper_mapping
group by x;
-- 首先选择外置脚本所需的数据列进行

select pm_ao,
       one_author[2]                               as last_name,
       one_author[3]                               as first_name,
       one_author[4]                               as initials,
       replaceAll(lower(mag_author_name), '.', '') as mag_author_name,
       replaceAll(lower(s2_author_name), '.', '')  as s2_author_name
from and.pm_aminer_s2_mag_paper_mapping
-- where toUInt32OrZero(substring(pm_ao, 2,1))=%d;
;

-- 转换结果存储
-- drop table and.pm_s2_mag_author_name_normalization;
create table if not exists and.pm_s2_mag_author_name_normalization
(
    pm_ao           String,
    last_name       String,
    first_name      String,
    initials        String,
    mag_author_name String,
    s2_author_name  String
) ENGINE = Log;

-- 验证转换结果的正确性
-- 119781963
select count(pm_ao)
from and.pm_s2_mag_author_name_normalization;

-- 119781963	119773647
select count(), count(distinct pm_ao)
from and.pm_s2_mag_author_name_normalization;

-- 119781963	119773647
select count(), count(distinct pm_ao)
from and.pm_aminer_s2_mag_paper_mapping;


-- 从合并转换结果中找到最完整的姓名，并修改原始的pubmed xml中获得的姓名。
create materialized view if not exists and.pm_aminer_s2_mag_paper_mapping_with_wellform_name ENGINE = Log populate as
select *,
       -- TODO 还需要替换 last_name
       [one_author_old_version[1], one_author_old_version[2], wellform_first_name, one_author_old_version[4]] as one_author
from (select pm_ao,
             pm_id,
             datetime_str,
             one_author as one_author_old_version,
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
      from and.pm_aminer_s2_mag_paper_mapping) any
         inner join (select pm_ao, wellform_name, wellform_first_name
                     from (select pm_ao,
                                  length(mag_author_name) >
                                  length(s2_author_name) ? mag_author_name : s2_author_name           as wellform_name,
                                  position(wellform_name, last_name)                                  as pos,
                                  -- [pos, pos+length(one_author[2])] -> ''
                                  trimBoth(concat(substring(wellform_name, 1, pos - 1),
                                                  substring(wellform_name, pos + length(last_name)))) as tmp_first_name,
                                  if(pos > 0 and
                                     length(tmp_first_name) > length(first_name),
                                     tmp_first_name,
                                     first_name)                                                      as wellform_first_name,
                                  first_name,
                                  first_name == wellform_first_name                                   as x
                           from and.pm_s2_mag_author_name_normalization)) using pm_ao;

-- 增加mag和s2之后，验证名字缩写还原任务的性能提升，性能提升比较明显，基本上所有的作者的完整的名字都可以被找到
-- pubmed 数据集中的具有完整的名字（非缩写）的比例：69729264/119781963=0.5821349246046335
select count()
from and.pm_aminer_s2_mag_paper_mapping
where length(arrayReverseSort(x->length(x), splitByChar(' ', one_author[3]))[1]) > 1;
-- 增强后pubmed 数据集中的具有完整的名字（非缩写）的比例：109021391/119781963=0.9101653393341033, 从58%增强到91%
select count()
from and.pm_aminer_s2_mag_paper_mapping_with_wellform_name
where length(arrayReverseSort(x->length(x), splitByChar(' ', one_author[3]))[1]) > 1;

-- 验证 initial 是否需要根据获得的较为完整的 firstname 进行更新，不需要更新。
-- 4643 of 13701438
select count()
from and.pm_aminer_s2_mag_paper_mapping_with_wellform_name
where length(one_author[4]) == 0
  and length(one_author[3]) != 0;