-- drop table and.SONG_GS_WHU_mix_dataset_clean_inner_pubmed_content;
create materialized view if not exists and.SONG_GS_WHU_mix_dataset_clean_inner_pubmed_content ENGINE = Log populate as
select pm_id,
       JSONExtractString(cast(journal, 'String'), 'nlmUniqueID')                     as journal_nlmUniqueID,
       JSONExtractString(cast(journal, 'String'), 'title')                           as journal_title,
       arrayStringConcat(
               extractAll(CAST(if(article_title is NULL, '', article_title), 'String'), '\\w+'),
               ' ')                                                                  as clean_title,
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
                         ' ')                                                        as clean_abstract,
       arrayStringConcat(arrayFilter(x -> x not in
                                          ('descriptorNameUI', 'descriptorName', 'majorTopicYN',
                                           'qualifierNameList',
                                           'false', 'true', 'null') and not match(x, '\\d+'),
                                     extractAll(
                                             CAST(if(mesh_headings is NULL, '', mesh_headings), 'String'),
                                             '\\w+')),
                         ' ')                                                        as clean_mesh_headings,
       arrayStringConcat(arrayFilter(x -> x not in
                                          ('keyword', 'majorTopicYN', 'false', 'true', 'null') and
                                          not match(x, '\\d+'),
                                     extractAll(
                                             CAST(if(keywords is NULL, '', keywords), 'String'),
                                             '\\w+')),
                         ' ')                                                        as clean_keywords,
       authors,
       arrayMap(x->
--                           arrayStringConcat(
                    [JSONExtractString(x, 'lastName'), JSONExtractString(x, 'foreName'), JSONExtractString(x, 'initials'), JSONExtractString(x, 'aff')],
--                  '###'),
                JSONExtractArrayRaw(cast(authors, 'String'), 'authorList'))          as author_list,
       arrayMap(x->concat(x[1], '_', substring(x[3], 1, 1)),
                author_list)                                                         as co_authors_by_name,
       arrayMap(x-> (x, author_list[x]),
                arrayEnumerate(author_list))                                         as author_list_with_order,
       arrayMap(x->replaceAll(x, '"', ''),
                JSONExtractArrayRaw(cast(languages, 'String')))                      as languages,
       CAST(if(vernacular_title is NULL, '', mesh_headings), 'String')               as vernacular_title,
       arrayMap(x -> extract(x, '\\d+'),
                extractAll(cast(references, 'String'),
                           '"articleId":"\\d+","idType":"pubmed"'))                  as cited_pm_id,
       arrayMap(x->replaceAll(x, '"', ''),
                JSONExtractArrayRaw(cast(data_banks, 'String'), 'dataBankNameList')) as databank_list,
       arrayMap(x->JSONExtractString(x, 'agency'),
                JSONExtractArrayRaw(cast(grants, 'String'), 'grantList'))            as grant_agency_list,
       arrayMap(x->JSONExtractString(x, 'country'),
                JSONExtractArrayRaw(cast(grants, 'String'), 'grantList'))            as grant_country_list,
       arrayMap(x->replaceAll(x, '"', ''),
                JSONExtractArrayRaw(cast(publication_types, 'String'),
                                    'publicationTypeList'))                          as pub_type_list,
       arrayMap(x->replaceAll(x, '"', ''),
                JSONExtractArrayRaw(cast(publication_types, 'String'), 'uiList'))    as pub_type_ui_list,
       arrayMap(x->replaceAll(replace(x, 'language=', ''), '"', ''),
                extractAll(CAST(if(other_abstracts is NULL, '', article_title), 'String'),
                           'language="\\w+"'))                                       as langs_from_other_abstract,
       datetime_str,
       from
from (select id,
             toString(pm_id) as pm_id,
             journal,
             article_title,
             abstract_str,
             authors,
             languages,
             data_banks,
             grants,
             publication_types,
             vernacular_title,
             suppl_meshs,
             comments_corrections,
             mesh_headings,
             personal_name_subjects,
             other_abstracts,
             keywords,
             history,
             references,
             datetime_str,
             datetime
      from pubmed.nft_paper)
         any
         inner join and.two_dataset_related_pubmed_paper using pm_id;


-- cat ner_cased_affiliation.tsv | clickhouse-local --input-format=TSV --table='input' --structure="pm_ao String, cased_affiliation String, str1 String, str2 String"  --query="select pm_ao,\
-- cased_affiliation, arrayFilter(x->length(x)>0, splitByChar('|', str1!='null' ? str1 : '')) as ner_orgs, arrayFilter(x->length(x)>0, splitByChar('|', str2!='null' ? str2 : '')) as ner_locs from input" --format=Native | clickhouse-client --query='INSERT INTO and.SONG_GS_WHU_mix_dataset_author_cased_affiliation FORMAT Native' --port=9001 --password=root
-- drop table and.SONG_GS_WHU_mix_dataset_author_cased_affiliation;
create table if not exists and.SONG_GS_WHU_mix_dataset_author_cased_affiliation
(
    pm_ao             String,
    cased_affiliation String,
    ner_orgs          Array(String),
    ner_locs          Array(String)
) ENGINE = Log;

-- 47691
select count()
from and.SONG_GS_WHU_mix_dataset_author_cased_affiliation;

-- drop table and.pubmed_inner_feature;
create materialized view if not exists and.pubmed_inner_feature ENGINE = Log populate as
with (select sum(commonness) from and.lastname_firat_initial_commonness) as all_occuracies
select *, commonness * 1.0 / all_occuracies as ambiguity_score
from (
         select *
         from ( select *
                from (
                         select pm_id,
                                concat(pm_id, '_', author_order)                  as pm_ao,
                                toString(tupleElement(one_author, 1))             as author_order,
                                concat(lastname, '_', substring(firstname, 1, 1)) as ns,
                                tupleElement(one_author, 2)[1]                    as lastname,
                                tupleElement(one_author, 2)[2]                    as firstname,
                                tupleElement(one_author, 2)[3]                    as initial,
                                tupleElement(one_author, 2)[4]                    as affiliation,
                                journal_nlmUniqueID,
                                journal_title,
                                clean_title,
                                clean_abstract,
                                clean_mesh_headings,
                                clean_keywords,
                                co_authors_by_name,
                                languages,
                                vernacular_title,
                                cited_pm_id,
                                databank_list,
                                grant_agency_list,
                                grant_country_list,
                                pub_type_list,
                                pub_type_ui_list,
                                langs_from_other_abstract,
                                datetime_str,
                                from,
                                jd,
                                st
                         from (select pm_id,
                                      journal_nlmUniqueID,
                                      journal_title,
                                      clean_title,
                                      clean_abstract,
                                      clean_mesh_headings,
                                      clean_keywords,
                                      authors,
                                      author_list,
                                      co_authors_by_name,
                                      author_list_with_order,
                                      languages,
                                      vernacular_title,
                                      cited_pm_id,
                                      databank_list,
                                      grant_agency_list,
                                      grant_country_list,
                                      pub_type_list,
                                      pub_type_ui_list,
                                      langs_from_other_abstract,
                                      datetime_str,
                                      from,
                                      jd,
                                      st
                               from and.SONG_GS_WHU_mix_dataset_clean_inner_pubmed_content any
                                        left join and.SONG_GS_WHU_mix_dataset_infer_JD_ST using pm_id)
                                  array join author_list_with_order as one_author) any
                         left join and.SONG_GS_WHU_mix_dataset_author_cased_affiliation using pm_ao)
                  any
                  left join (select concat(toString(PMID), '_', toString(AuthorRank))          as pm_ao,
                                    arrayFilter(x->length(x) > 0,
                                                groupUniqArray(lower(AffiliationIdentifier)))  as affiliation_ids,
                                    arrayFilter(x->length(x) > 0,
                                                groupUniqArray(lower(Department)))             as departments,
                                    arrayFilter(x->length(x) > 0,
                                                groupUniqArray(lower(Institution)))            as institutions,
                                    arrayFilter(x->length(x) > 0,
                                                groupUniqArray(lower(Email)))                  as emails,
                                    arrayFilter(x->length(x) > 0,
                                                groupUniqArray(lower(Zipcode)))                as zipcodes,
                                    arrayFilter(x->length(x) > 0,
                                                groupUniqArray(lower(Location)))               as locations,
                                    arrayFilter(x->length(x) > 0,
                                                groupUniqArray(lower(Country)))                as countrys,
                                    arrayFilter(x->length(x) > 0,
                                                groupUniqArray(lower(city)))                   as citys,
                                    arrayFilter(x->length(x) > 0,
                                                groupUniqArray(lower(state)))                  as states,
                                    arrayFilter(x->length(x) > 0,
                                                groupUniqArray(lower(mcountry)))               as mcountrys,
                                    arrayFilter(x->length(x) > 0, groupUniqArray(lower(type))) as types
                             from (select *, toString(PMID) as pm_id
                                   from and.C03_Affiliation_merge any
                                            inner join and.two_dataset_related_pubmed_paper using pm_id)
                             group by pm_ao) using pm_ao)
         any
         left join (select lastname_firat_initial as ns, commonness from and.lastname_firat_initial_commonness)
                   using ns;

select count()
from and.pubmed_inner_feature;

select distinct pm_id
from and.pubmed_inner_feature
where from = 'WHU'
into outfile 'WHU_distinct_pm_id.tsv' FORMAT TSV;

select pm_id,
       any(arrayStringConcat(
               [journal_nlmUniqueID, journal_title, clean_title, clean_abstract, clean_mesh_headings, clean_keywords, vernacular_title],
               ' ')) as content,
       any(from)     as source
from and.pubmed_inner_feature
group by pm_id
into outfile 'pubmed_all_text_content.tsv' FORMAT TSV;

-- reproduce Appendix A. in SONG's paper
select pm_id,
       journal_title,
       clean_title,
       datetime_str,
       lastname,
       firstname,
       initial,
       affiliation
from and.pubmed_inner_feature
where pm_id in
      ('19584133', '20801585', '21391934', '22224886', '22390578', '22390591', '22390593', '23597351', '24146116',
       '9197549', '9581716', '9714130', '11152845', '16415199', '18279736', '20368475', '20713894')
  and author_order <= '1';

create view if not exists and.SONG_GS_WHU_mix_dataset as
select concat(pm_id1, '_', matched_author_order1) as pm_ao1,
       concat(pm_id2, '_', matched_author_order2) as pm_ao2,
       same_author,
       'SONG'                                     as from
from and.SONG_dataset
union all
select concat(pm_id1, '_', matched_author_order1) as pm_ao1,
       concat(pm_id2, '_', matched_author_order2) as pm_ao2,
       same_author,
       'GS'                                       as from
from and.GS_dataset
union all
select concat(pm_id1, '_', matched_author_order1) as pm_ao1,
       concat(pm_id2, '_', matched_author_order2) as pm_ao2,
       same_author,
       'WHU'                                      as from
from and.WHU_dataset
;

select count(), count(distinct pm_id)
from and.pubmed_inner_feature;
-- 使用maui提取关键词的语料库

select pm_id, any(concat(clean_title, '. ', clean_abstract)) as title_abstract
from and.pubmed_inner_feature
group by pm_id
into OUTFILE 'SONG_GS_WHU_mix_dataset_pubmed_title_abstract.tsv' FORMAT TSV;

-- cat SONG_GS_WHU_mix_dataset_pubmed_extracted_keywords.tsv | clickhouse-local --input-format=TSV --table='input' --structure="pm_id String, kws String"  --query="select pm_id,\
-- arrayFilter(x->length(x) > 0, splitByChar('|', kws)) as extract_keywords from input" --format=Native | clickhouse-client --query='INSERT INTO and.SONG_GS_WHU_mix_dataset_extracted_keywords FORMAT Native' --port=9001 --password=root
-- drop table and.SONG_GS_WHU_mix_dataset_extracted_keywords;
create table if not exists and.SONG_GS_WHU_mix_dataset_extracted_keywords
(
    pm_id            String,
    extract_keywords Array(String)
) ENGINE = Log;

-- 40055
select count()
from and.SONG_GS_WHU_mix_dataset_extracted_keywords;

-- cat jd_st.tsv | clickhouse-local --input-format=TSV --table='input' --structure="pm_id String, str1 String, str2 String"  --query="select pm_id,\
-- arrayFilter(x->length(x) > 0, splitByChar('|', str1)) as jd, arrayFilter(x->length(x) > 0, splitByChar('|', str2)) as st from input" --format=Native | clickhouse-client --query='INSERT INTO and.SONG_GS_WHU_mix_dataset_infer_JD_ST FORMAT Native' --port=9001 --password=root
-- drop table and.SONG_GS_WHU_mix_dataset_infer_JD_ST;
create table if not exists and.SONG_GS_WHU_mix_dataset_infer_JD_ST
(
    pm_id String,
    jd    Array(String),
    st    Array(String)
) ENGINE = Log;

-- 40117
select count()
from and.SONG_GS_WHU_mix_dataset_infer_JD_ST;

desc and.pubmed_inner_feature;
desc and.SONG_GS_WHU_mix_dataset_pubmed_inner_feature;
select same_author, count()
from and.SONG_dataset
group by same_author;
-- 0.1868962620747585
select 28925 / 154765;

-- cat C03_Affiliation_merge.tsv | clickhouse-client --password root --port 9001 --query='insert into and.C03_Affiliation_merge FORMAT TSV'
create table and.C03_Affiliation_merge
(
    id                    Int32,
    ArticleID             Nullable(Int32),
    PMID                  Nullable(Int32),
    AuthorRank            Nullable(Int32),
    AffiliationRank       Nullable(Int32),
    AffiliationIdentifier Nullable(String),
    Affiliation           Nullable(String),
    Department            Nullable(String),
    Institution           Nullable(String),
    Email                 Nullable(String),
    Zipcode               Nullable(String),
    Location              Nullable(String),
    Country               Nullable(String),
    aid                   Nullable(Int32),
    city                  Nullable(String),
    state                 Nullable(String),
    mcountry              Nullable(String),
    type                  Nullable(String)
) engine = Log;


select count()
from and.C03_Affiliation_merge
union all
select count()
from and.C03_Affiliation_merge
where length(city) == 0
   or length(state) = 0
   or length(mcountry) = 0
   or length(type) = 0;

-- drop table and.SONG_GS_WHU_mix_dataset_external_author_id;
create view if not exists and.SONG_GS_WHU_mix_dataset_external_author_id_tmp as
select pm_ao1,
       pm_ao2,
       same_author,
       from                                                                             as source,
       length(aminer_author_id1) > 0 and length(aminer_author_id2) >
                                         0 ? aminer_author_id1 == aminer_author_id2: -1 as same_aminer_author_id,
       mag_author_id1 > 0 and
       mag_author_id2 > 0 ? mag_author_id1 == mag_author_id2 : -1                       as same_mag_author_id,
       length(s2_author_id1) > 0 and
       length(s2_author_id2) > 0 ? s2_author_id1 == s2_author_id2 : -1                  as same_s2_author_id,
       length(pkg_aid_v21) > 0 and
       length(pkg_aid_v22) > 0 ? pkg_aid_v21 == pkg_aid_v22 : -1                        as same_pkg_aid_v2_author_id
from (select same_author,
             mag_author_id_list_with_orders1,
             s2_author_id_list_with_orders1,
             pkg_aid_v1_coauthors1,
             pkg_aid_v2_coauthors1,
             vetle_aid_coauthors1,
             mag_author_id_list_with_orders2,
             s2_author_id_list_with_orders2,
             pkg_aid_v1_coauthors2,
             pkg_aid_v2_coauthors2,
             vetle_aid_coauthors2,
             pm_ao1,
             aminer_author_id1,
             s2_author_id1,
             mag_author_id1,
             vetle_aid1,
             pkg_aid_v11,
             pkg_aid_v21,
             pm_ao2,
             aminer_author_id2,
             s2_author_id2,
             mag_author_id2,
             vetle_aid2,
             pkg_aid_v12,
             pkg_aid_v22,
             'GS' as from
      from and.GS_dataset_rich_features
      union all
      select same_author,
             mag_author_id_list_with_orders1,
             s2_author_id_list_with_orders1,
             pkg_aid_v1_coauthors1,
             pkg_aid_v2_coauthors1,
             vetle_aid_coauthors1,
             mag_author_id_list_with_orders2,
             s2_author_id_list_with_orders2,
             pkg_aid_v1_coauthors2,
             pkg_aid_v2_coauthors2,
             vetle_aid_coauthors2,
             pm_ao1,
             aminer_author_id1,
             s2_author_id1,
             mag_author_id1,
             vetle_aid1,
             pkg_aid_v11,
             pkg_aid_v21,
             pm_ao2,
             aminer_author_id2,
             s2_author_id2,
             mag_author_id2,
             vetle_aid2,
             pkg_aid_v12,
             pkg_aid_v22,
             'SONG' as from
      from and.SONG_dataset_rich_features
      union all
      select same_author,
             mag_author_id_list_with_orders1,
             s2_author_id_list_with_orders1,
             pkg_aid_v1_coauthors1,
             pkg_aid_v2_coauthors1,
             vetle_aid_coauthors1,
             mag_author_id_list_with_orders2,
             s2_author_id_list_with_orders2,
             pkg_aid_v1_coauthors2,
             pkg_aid_v2_coauthors2,
             vetle_aid_coauthors2,
             pm_ao1,
             aminer_author_id1,
             s2_author_id1,
             mag_author_id1,
             vetle_aid1,
             pkg_aid_v11,
             pkg_aid_v21,
             pm_ao2,
             aminer_author_id2,
             s2_author_id2,
             mag_author_id2,
             vetle_aid2,
             pkg_aid_v12,
             pkg_aid_v22,
             'WHU' as from
      from and.WHU_dataset_rich_features
         );

create view if not exists and.SONG_GS_WHU_mix_dataset_external_author_id as
select *
from (
      select *
      from and.SONG_GS_WHU_mix_dataset_external_author_id_tmp
      where source == 'SONG'
        and same_author == 0
      order by xxHash64(concat(pm_ao1, toString(now64())))
      limit 43505
      union all
      select *
      from and.SONG_GS_WHU_mix_dataset_external_author_id_tmp
      where source == 'SONG'
        and same_author == 1
      order by xxHash64(concat(pm_ao2, toString(now64())))
      limit 14813
      union all
      select *
      from and.SONG_GS_WHU_mix_dataset_external_author_id_tmp
      where source == 'GS'
         );

-- GS	0	639
-- GS	1	1090
-- SONG	1	14813
-- SONG	0	43505
select source, same_author, count()
from and.SONG_GS_WHU_mix_dataset_external_author_id
group by source, same_author;



-- drop table and.SONG_GS_WHU_mix_dataset_pubmed_outer_feature;
create view if not exists and.SONG_GS_WHU_mix_dataset_pubmed_outer_feature as
select pm_ao1,
       pm_ao2,
       from                                                                     as source,
       outer_lastname1,
       outer_firstname1,
       outer_initials1,
       outer_lastname2,
       outer_firstname2,
       outer_initials2,
       length(aminer_author_id1) > 0 and aminer_author_id1 == aminer_author_id2 as same_author_id,
       mag_author_id1 > 0 and mag_author_id1 == mag_author_id2                  as same_mag_author_id,
       length(s2_author_id1) > 0 and s2_author_id1 == s2_author_id2             as same_s2_author_id,
       length(vetle_aid1) > 0 and vetle_aid1 == vetle_aid2                      as same_vetle_aid_author_id,
       length(pkg_aid_v11) > 0 and pkg_aid_v11 == pkg_aid_v12                   as same_pkg_aid_v1_author_id,
       length(pkg_aid_v21) > 0 and pkg_aid_v21 == pkg_aid_v22                   as same_pkg_aid_v2_author_id,
       length(arrayIntersect(mag_coauthor_ids1, mag_coauthor_ids2))             as num_mag_coauthors,
       length(arrayIntersect(s2_coauthor_ids1, s2_coauthor_ids2))               as num_s2_coauthors,
       length(arrayIntersect(vetle_coauthor_ids1, vetle_coauthor_ids2))         as num_vetel_coauthors,
       length(arrayIntersect(pkg_v1_coauthor_ids1, pkg_v1_coauthor_ids2))       as num_pkg_v1_coauthors,
       length(arrayIntersect(pkg_coauthor_ids1, pkg_coauthor_ids2))             as num_pkg_v2_coauthors
from (
      select pm_ao1,
             pm_ao2,
             from,
             length(lastname1) > length(one_author1[2]) ? lastname1: one_author1[2]                                   as outer_lastname1,
             length(firstname1) >
             length(one_author1[3]) ? firstname1: one_author1[3]                                                      as outer_firstname1,
             length(initials1) >
             length(one_author1[4]) ? initials1: one_author1[4]                                                       as outer_initials1,
             length(lastname2) >
             length(one_author2[2]) ? lastname2: one_author2[2]                                                       as outer_lastname2,
             length(firstname2) >
             length(one_author2[3]) ? firstname2: one_author2[3]                                                      as outer_firstname2,
             length(initials2) >
             length(one_author2[4]) ? initials2: one_author2[4]                                                       as outer_initials2,
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
             arrayDistinct(splitByChar(' ', arrayStringConcat([clean_title1, clean_keywords1, clean_mesh_headings1],
                                                              ' ')))                                                  as paper_words1,
             arrayDistinct(splitByChar(' ', arrayStringConcat([clean_title2, clean_keywords2, clean_mesh_headings2],
                                                              ' ')))                                                  as paper_words2,
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
      from (select one_author1,
                   one_author2,
                   lastname1,
                   initials1,
                   firstname1,
                   lastname2,
                   initials2,
                   firstname2,
                   mag_author_id_list_with_orders1,
                   s2_author_id_list_with_orders1,
                   pkg_aid_v1_coauthors1,
                   pkg_aid_v2_coauthors1,
                   vetle_aid_coauthors1,
                   mag_author_id_list_with_orders2,
                   s2_author_id_list_with_orders2,
                   pkg_aid_v1_coauthors2,
                   pkg_aid_v2_coauthors2,
                   vetle_aid_coauthors2,
                   pm_ao1,
                   aminer_author_id1,
                   s2_author_id1,
                   mag_author_id1,
                   vetle_aid1,
                   pkg_aid_v11,
                   pkg_aid_v21,
                   pm_ao2,
                   aminer_author_id2,
                   s2_author_id2,
                   mag_author_id2,
                   vetle_aid2,
                   pkg_aid_v12,
                   pkg_aid_v22,
                   clean_title1,
                   clean_keywords1,
                   clean_mesh_headings1,
                   clean_title2,
                   clean_keywords2,
                   clean_mesh_headings2,
                   'GS' as from
            from and.GS_dataset_rich_features
            union all
            select one_author1,
                   one_author2,
                   lastname1,
                   initials1,
                   firstname1,
                   lastname2,
                   initials2,
                   firstname2,
                   mag_author_id_list_with_orders1,
                   s2_author_id_list_with_orders1,
                   pkg_aid_v1_coauthors1,
                   pkg_aid_v2_coauthors1,
                   vetle_aid_coauthors1,
                   mag_author_id_list_with_orders2,
                   s2_author_id_list_with_orders2,
                   pkg_aid_v1_coauthors2,
                   pkg_aid_v2_coauthors2,
                   vetle_aid_coauthors2,
                   pm_ao1,
                   aminer_author_id1,
                   s2_author_id1,
                   mag_author_id1,
                   vetle_aid1,
                   pkg_aid_v11,
                   pkg_aid_v21,
                   pm_ao2,
                   aminer_author_id2,
                   s2_author_id2,
                   mag_author_id2,
                   vetle_aid2,
                   pkg_aid_v12,
                   pkg_aid_v22,
                   clean_title1,
                   clean_keywords1,
                   clean_mesh_headings1,
                   clean_title2,
                   clean_keywords2,
                   clean_mesh_headings2,
                   'SONG' as from
            from and.SONG_dataset_rich_features
            union all
            select one_author1,
                   one_author2,
                   lastname1,
                   initials1,
                   firstname1,
                   lastname2,
                   initials2,
                   firstname2,
                   mag_author_id_list_with_orders1,
                   s2_author_id_list_with_orders1,
                   pkg_aid_v1_coauthors1,
                   pkg_aid_v2_coauthors1,
                   vetle_aid_coauthors1,
                   mag_author_id_list_with_orders2,
                   s2_author_id_list_with_orders2,
                   pkg_aid_v1_coauthors2,
                   pkg_aid_v2_coauthors2,
                   vetle_aid_coauthors2,
                   pm_ao1,
                   aminer_author_id1,
                   s2_author_id1,
                   mag_author_id1,
                   vetle_aid1,
                   pkg_aid_v11,
                   pkg_aid_v21,
                   pm_ao2,
                   aminer_author_id2,
                   s2_author_id2,
                   mag_author_id2,
                   vetle_aid2,
                   pkg_aid_v12,
                   pkg_aid_v22,
                   clean_title1,
                   clean_keywords1,
                   clean_mesh_headings1,
                   clean_title2,
                   clean_keywords2,
                   clean_mesh_headings2,
                   'WHU' as from
            from and.WHU_dataset_rich_features
               )
         );


-- drop table and.SONG_GS_WHU_mix_dataset_pubmed_inner_feature;
create materialized view if not exists and.SONG_GS_WHU_mix_dataset_pubmed_inner_feature ENGINE = Log populate as
select *
from (
         select *
         from (
                  select *
                  from (
                           select *,
                                  lastname1                       as shared_lastname,
                                  xxHash32(shared_lastname) % 100 as lastname_hash_partition_for_split
                           from (
                                    select pm_ao1, pm_ao2, same_author, from
                                    from and.SONG_GS_WHU_mix_dataset)
                                    any
                                    inner join (select pm_id                     as pm_id1,
                                                       pm_ao                     as pm_ao1,
                                                       author_order              as author_order1,
                                                       lastname                  as lastname1,
                                                       firstname                 as firstname1,
                                                       initial                   as initial1,
                                                       affiliation               as affiliation1,
                                                       journal_nlmUniqueID       as journal_nlmUniqueID1,
                                                       journal_title             as journal_title1,
                                                       clean_title               as clean_title1,
                                                       clean_abstract            as clean_abstract1,
                                                       clean_mesh_headings       as clean_mesh_headings1,
                                                       clean_keywords            as clean_keywords1,
                                                       co_authors_by_name        as co_authors_by_name1,
                                                       languages                 as languages1,
                                                       vernacular_title          as vernacular_title1,
                                                       cited_pm_id               as cited_pm_id1,
                                                       databank_list             as databank_list1,
                                                       grant_agency_list         as grant_agency_list1,
                                                       grant_country_list        as grant_country_list1,
                                                       pub_type_list             as pub_type_list1,
                                                       pub_type_ui_list          as pub_type_ui_list1,
                                                       langs_from_other_abstract as langs_from_other_abstract1,
                                                       datetime_str              as datetime_str1,
                                                       ns                        as ns1,
                                                       jd                        as jd1,
                                                       st                        as st1,
                                                       cased_affiliation         as cased_affiliation1,
                                                       ner_orgs                  as ner_orgs1,
                                                       ner_locs                  as ner_locs1,
                                                       affiliation_ids           as affiliation_ids1,
                                                       departments               as departments1,
                                                       institutions              as institutions1,
                                                       emails                    as emails1,
                                                       zipcodes                  as zipcodes1,
                                                       locations                 as locations1,
                                                       countrys                  as countrys1,
                                                       citys                     as citys1,
                                                       states                    as states1,
                                                       mcountrys                 as mcountrys1,
                                                       types                     as types1,
                                                       commonness                as commonness1,
                                                       ambiguity_score           as ambiguity_score1
                                                from and.pubmed_inner_feature) using pm_ao1)
                           any
                           inner join (select pm_id                     as pm_id2,
                                              pm_ao                     as pm_ao2,
                                              author_order              as author_order2,
                                              lastname                  as lastname2,
                                              firstname                 as firstname2,
                                              initial                   as initial2,
                                              affiliation               as affiliation2,
                                              journal_nlmUniqueID       as journal_nlmUniqueID2,
                                              journal_title             as journal_title2,
                                              clean_title               as clean_title2,
                                              clean_abstract            as clean_abstract2,
                                              clean_mesh_headings       as clean_mesh_headings2,
                                              clean_keywords            as clean_keywords2,
                                              co_authors_by_name        as co_authors_by_name2,
                                              languages                 as languages2,
                                              vernacular_title          as vernacular_title2,
                                              cited_pm_id               as cited_pm_id2,
                                              databank_list             as databank_list2,
                                              grant_agency_list         as grant_agency_list2,
                                              grant_country_list        as grant_country_list2,
                                              pub_type_list             as pub_type_list2,
                                              pub_type_ui_list          as pub_type_ui_list2,
                                              langs_from_other_abstract as langs_from_other_abstract2,
                                              datetime_str              as datetime_str2,
                                              ns                        as ns2,
                                              jd                        as jd2,
                                              st                        as st2,
                                              cased_affiliation         as cased_affiliation2,
                                              ner_orgs                  as ner_orgs2,
                                              ner_locs                  as ner_locs2,
                                              affiliation_ids           as affiliation_ids2,
                                              departments               as departments2,
                                              institutions              as institutions2,
                                              emails                    as emails2,
                                              zipcodes                  as zipcodes2,
                                              locations                 as locations2,
                                              countrys                  as countrys2,
                                              citys                     as citys2,
                                              states                    as states2,
                                              mcountrys                 as mcountrys2,
                                              types                     as types2,
                                              commonness                as commonness2,
                                              ambiguity_score           as ambiguity_score2
                                       from and.pubmed_inner_feature) using pm_ao2) any
                  left join (select pm_id as pm_id1, extract_keywords as extract_keywords1
                             from and.SONG_GS_WHU_mix_dataset_extracted_keywords)
                            using pm_id1) any
         left join (select pm_id as pm_id2, extract_keywords as extract_keywords2
                    from and.SONG_GS_WHU_mix_dataset_extracted_keywords)
                   using pm_id2
;

select *
from (
         select pm_id1,
                pm_id2,
                lastname1,
                firstname1,
                initial1,
                lastname2,
                firstname2,
                initial2
         from and.SONG_GS_WHU_mix_dataset_pubmed_inner_feature
         where from = 'SONG') any
         inner join (select pm_id1, [lastname1, initials1] as LI1, pm_id2, [lastname2, initials2] as LI2
                     from and.SONG_dataset) using (pm_id1, pm_id2)
where lastname1 != LI1[1]
   or lastname2 != LI2[1];

select lastname_hash_partition_for_split, count()
from and.SONG_GS_WHU_mix_dataset_pubmed_inner_feature
group by lastname_hash_partition_for_split;

-- drop table and.SONG_GS_WHU_mix_dataset_pubmed_inner_outer_feature;
create materialized view if not exists and.SONG_GS_WHU_mix_dataset_pubmed_inner_outer_feature_temp ENGINE = Log populate as
select *
from and.SONG_GS_WHU_mix_dataset_pubmed_inner_feature
         any
         inner join and.SONG_GS_WHU_mix_dataset_pubmed_outer_feature using (pm_ao1, pm_ao2);

-- drop table and.SONG_GS_WHU_mix_dataset_pubmed_inner_outer_feature;
create view if not exists and.SONG_GS_WHU_mix_dataset_pubmed_inner_outer_feature as
select *
from (
      select *
      from and.SONG_GS_WHU_mix_dataset_pubmed_inner_outer_feature_temp
      where source == 'SONG'
        and same_author == 0
      order by xxHash64(concat(pm_ao1, toString(now64())))
      limit 43505
      union all
      select *
      from and.SONG_GS_WHU_mix_dataset_pubmed_inner_outer_feature_temp
      where source == 'SONG'
        and same_author == 1
      order by xxHash64(concat(pm_ao1, toString(now64())))
      limit 14813
      union all
      select *
      from and.SONG_GS_WHU_mix_dataset_pubmed_inner_outer_feature_temp
      where source == 'GS'
         );

-- WHU,20674
-- SONG,183690
-- GS,1729
select from, count()
from and.SONG_GS_WHU_mix_dataset_pubmed_inner_feature
group by from;
-- WHU,20674
-- SONG,183690
-- GS,1729
select source, count()
from and.SONG_GS_WHU_mix_dataset_pubmed_outer_feature
group by source;
-- SONG	58318
-- GS	1729
select source, count()
from and.SONG_GS_WHU_mix_dataset_pubmed_inner_outer_feature
group by source;

-- SONG	0	43505
-- GS	0	639
-- SONG	1	14813
-- GS	1	1090
select source, same_author, count()
from and.SONG_GS_WHU_mix_dataset_pubmed_inner_outer_feature
group by source, same_author;

-- 0
select count()
from and.SONG_GS_WHU_mix_dataset_pubmed_inner_outer_feature
where from != source;-- 0

select (lastname1, firstname1),
       (lastname2, firstname2),
       (outer_lastname1, outer_firstname1),
       (outer_lastname2, outer_firstname2),
       same_author
from and.SONG_GS_WHU_mix_dataset_pubmed_inner_outer_feature
where source = 'SONG'
--   and (
--     firstname1 != outer_firstname1
--     or firstname2 != outer_firstname2
--     )
--   and position(outer_firstname1, firstname1) == 0
  and firstname2 == outer_lastname1
order by same_author
;

select concat(lastname1, ', ', firstname1),
       concat(outer_lastname1, ', ', outer_firstname1),
       concat(lastname2, ', ', firstname2),
       concat(outer_lastname2, ', ', outer_firstname2),
       same_author
from and.SONG_GS_WHU_mix_dataset_pubmed_inner_outer_feature;

select pm_ao1,
--        (lastname1, firstname1),
--        (outer_lastname1, outer_firstname1),
--        (lastname2, firstname2),
--        (outer_lastname2, outer_firstname2),
       (firstname1, outer_firstname1),
       (firstname2, outer_firstname2),
       same_author,
       source
from and.SONG_GS_WHU_mix_dataset_pubmed_inner_outer_feature
where source = 'GS'
  and (
--       lastname1 != outer_lastname1
--    or lastname2 != outer_lastname2;
            length(replaceAll(firstname1, ' ', '')) <= 2 and length(outer_firstname1) > 2
        or
            length(replaceAll(firstname2, ' ', '')) <= 2 and length(outer_firstname2) > 2
    )
order by same_author
;


select length(arrayFilter(x->length(x) > 0, groupArray(pm_ao1)))                     as pm_ao1,
       length(arrayFilter(x->length(x) > 0, groupArray(pm_ao2)))                     as pm_ao2,
       length(arrayFilter(x->length(x) > 0, groupArray(pm_id1)))                     as pm_id1,
       length(arrayFilter(x->length(x) > 0, groupArray(author_order1)))              as author_order1,
       length(arrayFilter(x->length(x) > 0, groupArray(lastname1)))                  as lastname1,
       length(arrayFilter(x->length(x) > 0, groupArray(firstname1)))                 as firstname1,
       length(arrayFilter(x->length(x) > 0, groupArray(initial1)))                   as initial1,
       length(arrayFilter(x->length(x) > 0, groupArray(affiliation1)))               as affiliation1,
       length(arrayFilter(x->length(x) > 0, groupArray(journal_nlmUniqueID1)))       as journal_nlmUniqueID1,
       length(arrayFilter(x->length(x) > 0, groupArray(journal_title1)))             as journal_title1,
       length(arrayFilter(x->length(x) > 0, groupArray(clean_title1)))               as clean_title1,
       length(arrayFilter(x->length(x) > 0, groupArray(clean_abstract1)))            as clean_abstract1,
       length(arrayFilter(x->length(x) > 0, groupArray(clean_mesh_headings1)))       as clean_mesh_headings1,
       length(arrayFilter(x->length(x) > 0, groupArray(clean_keywords1)))            as clean_keywords1,
       length(arrayFilter(x->length(x) > 0, groupArray(vernacular_title1)))          as vernacular_title1,
       length(arrayFilter(x->length(x) > 0, groupArray(datetime_str1)))              as datetime_str1,
       length(arrayFilter(x->length(x) > 0, groupArray(ns1)))                        as ns1,
       length(arrayFilter(x->length(x) > 0, groupArray(cased_affiliation1)))         as cased_affiliation1,
       length(arrayFilter(x->length(x) > 0, groupArray(pm_id2)))                     as pm_id2,
       length(arrayFilter(x->length(x) > 0, groupArray(author_order2)))              as author_order2,
       length(arrayFilter(x->length(x) > 0, groupArray(lastname2)))                  as lastname2,
       length(arrayFilter(x->length(x) > 0, groupArray(firstname2)))                 as firstname2,
       length(arrayFilter(x->length(x) > 0, groupArray(initial2)))                   as initial2,
       length(arrayFilter(x->length(x) > 0, groupArray(affiliation2)))               as affiliation2,
       length(arrayFilter(x->length(x) > 0, groupArray(journal_nlmUniqueID2)))       as journal_nlmUniqueID2,
       length(arrayFilter(x->length(x) > 0, groupArray(journal_title2)))             as journal_title2,
       length(arrayFilter(x->length(x) > 0, groupArray(clean_title2)))               as clean_title2,
       length(arrayFilter(x->length(x) > 0, groupArray(clean_abstract2)))            as clean_abstract2,
       length(arrayFilter(x->length(x) > 0, groupArray(clean_mesh_headings2)))       as clean_mesh_headings2,
       length(arrayFilter(x->length(x) > 0, groupArray(clean_keywords2)))            as clean_keywords2,
       length(arrayFilter(x->length(x) > 0, groupArray(vernacular_title2)))          as vernacular_title2,
       length(arrayFilter(x->length(x) > 0, groupArray(datetime_str2)))              as datetime_str2,
       length(arrayFilter(x->length(x) > 0, groupArray(ns2)))                        as ns2,
       length(arrayFilter(x->length(x) > 0, groupArray(cased_affiliation2)))         as cased_affiliation2,
       length(arrayFilter(x->x > 0, groupArray(length(co_authors_by_name1))))        as co_authors_by_name1,
       length(arrayFilter(x->x > 0, groupArray(length(languages1))))                 as languages1,
       length(arrayFilter(x->x > 0, groupArray(length(cited_pm_id1))))               as cited_pm_id1,
       length(arrayFilter(x->x > 0, groupArray(length(databank_list1))))             as databank_list1,
       length(arrayFilter(x->x > 0, groupArray(length(grant_agency_list1))))         as grant_agency_list1,
       length(arrayFilter(x->x > 0, groupArray(length(grant_country_list1))))        as grant_country_list1,
       length(arrayFilter(x->x > 0, groupArray(length(pub_type_list1))))             as pub_type_list1,
       length(arrayFilter(x->x > 0, groupArray(length(pub_type_ui_list1))))          as pub_type_ui_list1,
       length(arrayFilter(x->x > 0, groupArray(length(langs_from_other_abstract1)))) as langs_from_other_abstract1, --
       length(arrayFilter(x->x > 0, groupArray(length(jd1))))                        as jd1,
       length(arrayFilter(x->x > 0, groupArray(length(st1))))                        as st1,
       length(arrayFilter(x->x > 0, groupArray(length(ner_orgs1))))                  as ner_orgs1,
       length(arrayFilter(x->x > 0, groupArray(length(ner_locs1))))                  as ner_locs1,
       length(arrayFilter(x->x > 0, groupArray(length(affiliation_ids1))))           as affiliation_ids1,           --
       length(arrayFilter(x->x > 0, groupArray(length(departments1))))               as departments1,
       length(arrayFilter(x->x > 0, groupArray(length(institutions1))))              as institutions1,
       length(arrayFilter(x->x > 0, groupArray(length(emails1))))                    as emails1,
       length(arrayFilter(x->x > 0, groupArray(length(zipcodes1))))                  as zipcodes1,
       length(arrayFilter(x->x > 0, groupArray(length(locations1))))                 as locations1,
       length(arrayFilter(x->x > 0, groupArray(length(countrys1))))                  as countrys1,
       length(arrayFilter(x->x > 0, groupArray(length(citys1))))                     as citys1,
       length(arrayFilter(x->x > 0, groupArray(length(states1))))                    as states1,
       length(arrayFilter(x->x > 0, groupArray(length(mcountrys1))))                 as mcountrys1,
       length(arrayFilter(x->x > 0, groupArray(length(types1))))                     as types1,
       length(arrayFilter(x->x > 0, groupArray(length(co_authors_by_name2))))        as co_authors_by_name2,
       length(arrayFilter(x->x > 0, groupArray(length(languages2))))                 as languages2,
       length(arrayFilter(x->x > 0, groupArray(length(cited_pm_id2))))               as cited_pm_id2,
       length(arrayFilter(x->x > 0, groupArray(length(databank_list2))))             as databank_list2,
       length(arrayFilter(x->x > 0, groupArray(length(grant_agency_list2))))         as grant_agency_list2,
       length(arrayFilter(x->x > 0, groupArray(length(grant_country_list2))))        as grant_country_list2,
       length(arrayFilter(x->x > 0, groupArray(length(pub_type_list2))))             as pub_type_list2,
       length(arrayFilter(x->x > 0, groupArray(length(pub_type_ui_list2))))          as pub_type_ui_list2,
       length(arrayFilter(x->x > 0, groupArray(length(langs_from_other_abstract2)))) as langs_from_other_abstract2,
       length(arrayFilter(x->x > 0, groupArray(length(jd2))))                        as jd2,
       length(arrayFilter(x->x > 0, groupArray(length(st2))))                        as st2,
       length(arrayFilter(x->x > 0, groupArray(length(ner_orgs2))))                  as ner_orgs2,
       length(arrayFilter(x->x > 0, groupArray(length(ner_locs2))))                  as ner_locs2,
       length(arrayFilter(x->x > 0, groupArray(length(affiliation_ids2))))           as affiliation_ids2,
       length(arrayFilter(x->x > 0, groupArray(length(departments2))))               as departments2,
       length(arrayFilter(x->x > 0, groupArray(length(institutions2))))              as institutions2,
       length(arrayFilter(x->x > 0, groupArray(length(emails2))))                    as emails2,
       length(arrayFilter(x->x > 0, groupArray(length(zipcodes2))))                  as zipcodes2,
       length(arrayFilter(x->x > 0, groupArray(length(locations2))))                 as locations2,
       length(arrayFilter(x->x > 0, groupArray(length(countrys2))))                  as countrys2,
       length(arrayFilter(x->x > 0, groupArray(length(citys2))))                     as citys2,
       length(arrayFilter(x->x > 0, groupArray(length(states2))))                    as states2,
       length(arrayFilter(x->x > 0, groupArray(length(mcountrys2))))                 as mcountrys2,
       length(arrayFilter(x->x > 0, groupArray(length(types2))))                     as types2,
       length(arrayFilter(x->x > 0, groupArray(length(extract_keywords1))))          as extract_keywords1,
       length(arrayFilter(x->x > 0, groupArray(length(extract_keywords2))))          as extract_keywords2
from and.SONG_GS_WHU_mix_dataset_pubmed_inner_feature;

desc and.SONG_GS_WHU_mix_dataset_pubmed_inner_outer_feature;
