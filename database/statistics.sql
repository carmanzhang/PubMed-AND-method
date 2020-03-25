-- number of publications of each year
-- drop table and.num_pub_yearly;
create view if not exists and.num_pub_yearly as
select toInt32(substring(datetime_str, 1, 4)) as year,
       count()                                as num_pubs,

       num_pubs / 10000                       as cnt,
       bar(cnt, 10, 127, 10)
from pubmed.nft_paper
where year >= 1970
  and year <= 2018
group by year
order by year
;

-- cumulative abbreviated first name, statistics of firstname completeness malformed
-- drop table and.num_malformed_name_cumulatively;
create view if not exists and.num_malformed_name_cumulatively as
select tupleElement(arrayJoin(
                            arrayZip(arrayMap(x->x[1], items),
                                     arrayCumSum(x->x[2], groupArray(item) as items))) as temp, 1) as year,
       tupleElement(temp, 2)                                                                       as num_abbr_first_name
from (
      select [toUInt64(substring(datetime_str, 1, 4)) as year,
                 count() as cnt] as item
      from and.nft_paper_author_name_list
               -- ['1','nau','jean-yves','jy']  ['{"valid":true,"lastName":"nau","foreName":"jean-yves","initials":"jy"}']
               array join author_list as one_author
      where length(arrayReverseSort(x->length(x), splitByChar(' ', one_author[3]))[1]) <= 1
        and year >= 1970
        and year <= 2018
      group by year
      order by year)
;

-- drop table and.num_author_with_aff_cumulatively;
create view if not exists and.num_author_with_aff_cumulatively as
select tupleElement(arrayJoin(
                            arrayZip(arrayMap(x->x[1], items),
                                     arrayCumSum(x->x[2], groupArray(item) as items),
                                     arrayCumSum(x->x[3], items)
                                )) as temp, 1) as year,
       tupleElement(temp, 2)                   as num_authors,
       tupleElement(temp, 3)                   as num_authors_with_aff
from (
      select [year,
                 sum(num_author_within_paper) as num_author_within_year,
                 sum(num_author_with_aff_within_paper) as num_author_with_aff_within_year] as item
      from (select toUInt64(substring(datetime_str, 1, 4))               as year,
                   length(arrayMap(x->length(JSONExtractString(x, 'aff')) > 0,
                                   authors_list_raw) as author_with_aff) as num_author_within_paper,
                   arrayCount(x->x > 0, author_with_aff)                 as num_author_with_aff_within_paper
            from and.nft_paper_author_name_list)
      where year >= 1970
        and year <= 2018
      group by year
      order by year)
;


select lastname_firat_initial as ns, commonness
from and.nft_paper_author_ns_commonness_with_id
where length(ns) > 1
order by commonness
;

-- drop table and.pubmed_metadata_existing;
create materialized view if not exists and.pubmed_metadata_existing ENGINE = Log populate as
select *
from (
      select arrayJoin(arrayMap(x->concat(toString(pm_id), '_', toString(x + 1)),
                                range(JSONLength(cast(authors, 'String'), 'authorList') as num_authors))) as pm_ao,
             abstract_str is NULL ? 0:1                                                                   as has_abstract,
             references == 'null' ? 0:1                                                                   as has_reference,
             keywords == 'null' ? 0:1                                                                     as has_keyword,
             arrayCount(x->x != '"eng"', JSONExtractArrayRaw(cast(languages, 'String'))) ==
             0 ? 0:1                                                                                      as has_lang,
             length(JSONExtractString(cast(journal, 'String'), 'title')) ==
             0 ? 0:1                                                                                      as has_journal_title,
             num_authors <= 1 ? 0:1                                                                       as coauthor_gt1,
             vernacular_title is null ? 0:1                                                               as has_vernacular_title,
             data_banks == 'null' ? 0:1                                                                   as has_databank,
             grants == 'null' ? 0:1                                                                       as has_grant,
             JSONLength(cast(publication_types, 'String'), 'publicationTypeList') ==
             0 ? 0:1                                                                                      as has_pub_type,
             length(datetime_str) == 0 ? 0:1                                                              as has_pub_date
      from pubmed.nft_paper);

create materialized view if not exists and.pubmed_metadata_existing_remove_bad_instances ENGINE = Log populate as
select *
from and.pubmed_metadata_existing
         any
         inner join (select pm_ao from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn) using pm_ao;

-- 122107254
select count()
from and.pubmed_metadata_existing;
-- 119781952
select count()
from and.pubmed_metadata_existing_remove_bad_instances;

-- drop table and.pubmed_metadata_sparsity;
-- running about 15 minutes
create view and.pubmed_metadata_sparsity as
select name, cnt
from (
      select count() as cnt, 'num_all_author' as name
      from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn
      UNION ALL
      select count() as cnt, 'num_valid_aminer_author_id' as name
      from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn
      where length(aminer_author_id) > 0
      UNION ALL
      select count() as cnt, 'num_valid_s2_author_id' as name
      from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn
      where length(s2_author_ids) > 0
      UNION ALL
      select count() as cnt, 'num_valid_mag_author_id' as name
      from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn
      where mag_author_id > 0
      UNION ALL
      select count() as cnt, 'num_valid_pkg_v1_author_id' as name
      from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn
      where pkg_aid_v1 != '0'
      UNION ALL
      select count() as cnt, 'num_valid_pkg_v2_author_id' as name
      from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn
      where pkg_aid_v2 != '0'
      UNION ALL
      select count() as cnt, 'num_valid_vetle_author_id' as name
      from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn
      where vetle_aid != '0'
      UNION ALL
      select count() as cnt, 'num_valid_paper_title' as name
      from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn
      where length(clean_title) > 0
      UNION ALL
      select count() as cnt, 'num_valid_mesh_heading' as name
      from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn
      where length(clean_mesh_headings) > 0
      UNION ALL
      select count() as cnt, 'num_valid_keyword' as name
      from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn
      where length(clean_keywords) > 0
      UNION ALL
      select count() as cnt, 'num_valid_aminer_co_author' as name
      from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn
      where length(arrayFilter(x->length(x) > 0, aminer_co_author_id)) > 0
      UNION ALL
      select count() as cnt, 'num_valid_s2_co_author' as name
      from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn
      where length(arrayFilter(x->length(x) > 0, s2_co_author_id)) > 0
      UNION ALL
      select count() as cnt, 'num_valid_mag_co_author' as name
      from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn
      where length(arrayFilter(x->x > 0, mag_co_author_id)) > 0
      UNION ALL
      select count() as cnt, 'num_valid_pkg_v1_co_author' as name
      from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn
      where length(arrayFilter(x->length(x) > 0, pkg_v1_co_author_id)) > 0
      UNION ALL
      select count() as cnt, 'num_valid_pkg_v2_co_author' as name
      from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn
      where length(arrayFilter(x->length(x) > 0, pkg_co_author_id)) > 0
      UNION ALL
      select count() as cnt, 'num_valid_vetel_co_author' as name
      from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn
      where length(arrayFilter(x->length(x) > 0, vetle_co_author_id)) > 0
      UNION ALL
      select count() as cnt, 'num_valid_completed_name' as name
      from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn
      where length(one_author[2]) > 0
        and length(one_author[3]) > 1
      UNION ALL
      select sum(length(arrayFilter(x->length(x[2]) > 0 and length(x[3]) > 1, author_list))) as cnt,
             'num_valid_original_name'                                                       as name
      from and.nft_paper_author_name_list
      UNION ALL
      select sum(length(arrayFilter(x->position(x, '@') > 0, authors_list_raw))) as cnt,
             'num_valid_email'                                                   as name
      from and.nft_paper_author_name_list
      UNION ALL
      select sum(length(arrayFilter(x->length(JSONExtractString(x, 'aff')) > 0, authors_list_raw))) as cnt,
             'num_valid_aff'                                                                        as name
      from and.nft_paper_author_name_list
      UNION ALL
      select sum(JSONLength(cast(authors, 'String'), 'authorList') * (abstract_str is NULL ? 0:1)) as cnt,
             'num_valid_has_abstract'                                                              as name
      from pubmed.nft_paper
      UNION ALL
      select sum(JSONLength(cast(authors, 'String'), 'authorList') * (references == 'null' ? 0:1)) as cnt,
             'num_valid_has_reference'                                                             as name
      from pubmed.nft_paper
      UNION ALL
      select sum(JSONLength(cast(authors, 'String'), 'authorList') * (keywords == 'null' ? 0:1)) as cnt,
             'num_valid_has_keyword'                                                             as name
      from pubmed.nft_paper
      UNION ALL
      select sum(JSONLength(cast(authors, 'String'), 'authorList') *
                 (arrayCount(x->x != '"eng"', JSONExtractArrayRaw(cast(languages, 'String'))) == 0 ? 0:1)) as cnt,
             'num_valid_has_lang'                                                                          as name
      from pubmed.nft_paper
           -- journal
      UNION ALL
      select sum(JSONLength(cast(authors, 'String'), 'authorList') *
                 (length(JSONExtractString(cast(journal, 'String'), 'title')) == 0 ? 0:1)) as cnt,
             'num_valid_has_journal_title'                                                 as name
      from pubmed.nft_paper
           -- co_authors > 0
      UNION ALL
      select sum(JSONLength(cast(authors, 'String'), 'authorList') *
                 (JSONLength(cast(authors, 'String'), 'authorList') <= 1 ? 0:1)) as cnt,
             'num_valid_coauthor_gt1'                                            as name
      from pubmed.nft_paper
           -- vernacular_title
      UNION ALL
      select sum(JSONLength(cast(authors, 'String'), 'authorList') *
                 (vernacular_title is null ? 0:1)) as cnt,
             'num_valid_has_vernacular_title'      as name
      from pubmed.nft_paper
           -- databank_list
      UNION ALL
      select sum(JSONLength(cast(authors, 'String'), 'authorList') *
                 (data_banks == 'null' ? 0:1)) as cnt,
             'num_valid_has_databank'          as name
      from pubmed.nft_paper
           -- grant
      UNION ALL
      select sum(JSONLength(cast(authors, 'String'), 'authorList') *
                 (grants == 'null' ? 0:1)) as cnt,
             'num_valid_has_grant'         as name
      from pubmed.nft_paper
           -- pub_type_list
      UNION ALL
      select sum(JSONLength(cast(authors, 'String'), 'authorList') *
                 (JSONLength(cast(publication_types, 'String'), 'publicationTypeList') == 0 ? 0:1)) as cnt,
             'num_valid_has_pub_type'                                                               as name
      from pubmed.nft_paper
           -- datetime_str
      UNION ALL
      select sum(JSONLength(cast(authors, 'String'), 'authorList') *
                 (length(datetime_str) == 0 ? 0:1)) as cnt,
             'num_valid_has_pub_date'               as name
      from pubmed.nft_paper);


-- drop table and.SONG_GS_unique_pm_id;
create materialized view if not exists and.SONG_GS_unique_pm_id ENGINE = Log populate as
select pm_id, sources
from (select toString(pm_id) as pm_id, authors from pubmed.nft_paper)
         any
         inner join (
    select item[1] as pm_id, groupUniqArray(item[2]) as sources
    from (
          with (select groupUniqArray(PMID)
                from and.SONG
                where length(PMID) > 0) as song_pm_id, (select groupUniqArray(pm_id)
                                                        from and.GS
                                                                 array join [PMID1, PMID2] as pm_id
                                                        where length(pm_id) > 0) as gs_pm_id
          select arrayJoin(arrayConcat(arrayMap(x->
                                                    [x, 'GS'], gs_pm_id), arrayMap(x->
                                                                                       [x, 'SONG'],
                                                                                   song_pm_id))) as item
             )
    group by item[1]
    having pm_id != 'null'
       and length(pm_id) > 0) using pm_id
where JSONLength(cast(authors, 'String'), 'authorList') <= 50
order by length(sources) desc
;

-- ['SONG']	2873
-- ['GS','SONG']	2
-- ['GS']	3699
select sources, count()
from and.SONG_GS_unique_pm_id
group by sources;

-- drop table and.SONG_dataset_metadata_sparsity;
create view if not exists and.SONG_dataset_metadata_sparsity as
select name, cnt
from (
      select count() as cnt, 'num_all_author' as name
      from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn
               any
               inner join (select pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'SONG') using pm_id
      UNION ALL
      select count() as cnt, 'num_valid_aminer_author_id' as name
      from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn any
               inner join (select pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'SONG') using pm_id
      where length(aminer_author_id) > 0
      UNION ALL
      select count() as cnt, 'num_valid_s2_author_id' as name
      from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn any
               inner join (select pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'SONG') using pm_id
      where length(s2_author_ids) > 0
      UNION ALL
      select count() as cnt, 'num_valid_mag_author_id' as name
      from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn any
               inner join (select pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'SONG') using pm_id
      where mag_author_id > 0
      UNION ALL
      select count() as cnt, 'num_valid_pkg_v1_author_id' as name
      from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn any
               inner join (select pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'SONG') using pm_id
      where pkg_aid_v1 != '0'
      UNION ALL
      select count() as cnt, 'num_valid_pkg_v2_author_id' as name
      from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn any
               inner join (select pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'SONG') using pm_id
      where pkg_aid_v2 != '0'
      UNION ALL
      select count() as cnt, 'num_valid_vetle_author_id' as name
      from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn any
               inner join (select pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'SONG') using pm_id
      where vetle_aid != '0'
      UNION ALL
      select count() as cnt, 'num_valid_paper_title' as name
      from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn any
               inner join (select pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'SONG') using pm_id
      where length(clean_title) > 0
      UNION ALL
      select count() as cnt, 'num_valid_mesh_heading' as name
      from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn any
               inner join (select pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'SONG') using pm_id
      where length(clean_mesh_headings) > 0
      UNION ALL
      select count() as cnt, 'num_valid_keyword' as name
      from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn any
               inner join (select pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'SONG') using pm_id
      where length(clean_keywords) > 0
      UNION ALL
      select count() as cnt, 'num_valid_aminer_co_author' as name
      from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn any
               inner join (select pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'SONG') using pm_id
      where length(arrayFilter(x->length(x) > 0, aminer_co_author_id)) > 0
      UNION ALL
      select count() as cnt, 'num_valid_s2_co_author' as name
      from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn any
               inner join (select pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'SONG') using pm_id
      where length(arrayFilter(x->length(x) > 0, s2_co_author_id)) > 0
      UNION ALL
      select count() as cnt, 'num_valid_mag_co_author' as name
      from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn any
               inner join (select pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'SONG') using pm_id
      where length(arrayFilter(x->x > 0, mag_co_author_id)) > 0
      UNION ALL
      select count() as cnt, 'num_valid_pkg_v1_co_author' as name
      from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn any
               inner join (select pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'SONG') using pm_id
      where length(arrayFilter(x->length(x) > 0, pkg_v1_co_author_id)) > 0
      UNION ALL
      select count() as cnt, 'num_valid_pkg_v2_co_author' as name
      from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn any
               inner join (select pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'SONG') using pm_id
      where length(arrayFilter(x->length(x) > 0, pkg_co_author_id)) > 0
      UNION ALL
      select count() as cnt, 'num_valid_vetel_co_author' as name
      from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn any
               inner join (select pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'SONG') using pm_id
      where length(arrayFilter(x->length(x) > 0, vetle_co_author_id)) > 0
      UNION ALL
      select count() as cnt, 'num_valid_completed_name' as name
      from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn any
               inner join (select pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'SONG') using pm_id
      where length(one_author[2]) > 0
        and length(one_author[3]) > 1
      UNION ALL
      select sum(length(arrayFilter(x->length(x[2]) > 0 and length(x[3]) > 1, author_list))) as cnt,
             'num_valid_original_name'                                                       as name
      from and.nft_paper_author_name_list any
               inner join (select pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'SONG') using pm_id
      UNION ALL
      select sum(length(arrayFilter(x->position(x, '@') > 0, authors_list_raw))) as cnt,
             'num_valid_email'                                                   as name
      from and.nft_paper_author_name_list any
               inner join (select pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'SONG') using pm_id
      UNION ALL
      select sum(length(arrayFilter(x->length(JSONExtractString(x, 'aff')) > 0, authors_list_raw))) as cnt,
             'num_valid_aff'                                                                        as name
      from and.nft_paper_author_name_list any
               inner join (select pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'SONG') using pm_id
      UNION ALL
      select sum(JSONLength(cast(authors, 'String'), 'authorList') * (abstract_str is NULL ? 0:1)) as cnt,
             'num_valid_has_abstract'                                                              as name
      from pubmed.nft_paper any
               inner join (select toUInt64(pm_id) as pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'SONG')
                          using pm_id
      UNION ALL
      select sum(JSONLength(cast(authors, 'String'), 'authorList') * (references == 'null' ? 0:1)) as cnt,
             'num_valid_has_reference'                                                             as name
      from pubmed.nft_paper any
               inner join (select toUInt64(pm_id) as pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'SONG')
                          using pm_id
      UNION ALL
      select sum(JSONLength(cast(authors, 'String'), 'authorList') * (keywords == 'null' ? 0:1)) as cnt,
             'num_valid_has_keyword'                                                             as name
      from pubmed.nft_paper any
               inner join (select toUInt64(pm_id) as pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'SONG')
                          using pm_id
      UNION ALL
      select sum(JSONLength(cast(authors, 'String'), 'authorList') *
                 (arrayCount(x->x != '"eng"', JSONExtractArrayRaw(cast(languages, 'String'))) == 0 ? 0:1)) as cnt,
             'num_valid_has_lang'                                                                          as name
      from pubmed.nft_paper any
               inner join (select toUInt64(pm_id) as pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'SONG')
                          using pm_id
           -- journal
      UNION ALL
      select sum(JSONLength(cast(authors, 'String'), 'authorList') *
                 (length(JSONExtractString(cast(journal, 'String'), 'title')) == 0 ? 0:1)) as cnt,
             'num_valid_has_journal_title'                                                 as name
      from pubmed.nft_paper any
               inner join (select toUInt64(pm_id) as pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'SONG')
                          using pm_id
           -- co_authors > 0
      UNION ALL
      select sum(JSONLength(cast(authors, 'String'), 'authorList') *
                 (JSONLength(cast(authors, 'String'), 'authorList') <= 1 ? 0:1)) as cnt,
             'num_valid_coauthor_gt1'                                            as name
      from pubmed.nft_paper any
               inner join (select toUInt64(pm_id) as pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'SONG')
                          using pm_id
           -- vernacular_title
      UNION ALL
      select sum(JSONLength(cast(authors, 'String'), 'authorList') *
                 (vernacular_title is null ? 0:1)) as cnt,
             'num_valid_has_vernacular_title'      as name
      from pubmed.nft_paper any
               inner join (select toUInt64(pm_id) as pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'SONG')
                          using pm_id
           -- databank_list
      UNION ALL
      select sum(JSONLength(cast(authors, 'String'), 'authorList') *
                 (data_banks == 'null' ? 0:1)) as cnt,
             'num_valid_has_databank'          as name
      from pubmed.nft_paper any
               inner join (select toUInt64(pm_id) as pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'SONG')
                          using pm_id
           -- grant
      UNION ALL
      select sum(JSONLength(cast(authors, 'String'), 'authorList') *
                 (grants == 'null' ? 0:1)) as cnt,
             'num_valid_has_grant'         as name
      from pubmed.nft_paper any
               inner join (select toUInt64(pm_id) as pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'SONG')
                          using pm_id
           -- pub_type_list
      UNION ALL
      select sum(JSONLength(cast(authors, 'String'), 'authorList') *
                 (JSONLength(cast(publication_types, 'String'), 'publicationTypeList') == 0 ? 0:1)) as cnt,
             'num_valid_has_pub_type'                                                               as name
      from pubmed.nft_paper any
               inner join (select toUInt64(pm_id) as pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'SONG')
                          using pm_id
           -- datetime_str
      UNION ALL
      select sum(JSONLength(cast(authors, 'String'), 'authorList') *
                 (length(datetime_str) == 0 ? 0:1)) as cnt,
             'num_valid_has_pub_date'               as name
      from pubmed.nft_paper any
               inner join (select toUInt64(pm_id) as pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'SONG')
                          using pm_id
         );


select *
from and.SONG_dataset_metadata_sparsity;

-- drop table and.GS_dataset_metadata_sparsity;
create view if not exists and.GS_dataset_metadata_sparsity as
select name, cnt
from (
      select count() as cnt, 'num_all_author' as name
      from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn
               any
               inner join (select pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'GS') using pm_id
      UNION ALL
      select count() as cnt, 'num_valid_aminer_author_id' as name
      from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn any
               inner join (select pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'GS') using pm_id
      where length(aminer_author_id) > 0
      UNION ALL
      select count() as cnt, 'num_valid_s2_author_id' as name
      from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn any
               inner join (select pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'GS') using pm_id
      where length(s2_author_ids) > 0
      UNION ALL
      select count() as cnt, 'num_valid_mag_author_id' as name
      from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn any
               inner join (select pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'GS') using pm_id
      where mag_author_id > 0
      UNION ALL
      select count() as cnt, 'num_valid_pkg_v1_author_id' as name
      from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn any
               inner join (select pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'GS') using pm_id
      where pkg_aid_v1 != '0'
      UNION ALL
      select count() as cnt, 'num_valid_pkg_v2_author_id' as name
      from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn any
               inner join (select pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'GS') using pm_id
      where pkg_aid_v2 != '0'
      UNION ALL
      select count() as cnt, 'num_valid_vetle_author_id' as name
      from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn any
               inner join (select pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'GS') using pm_id
      where vetle_aid != '0'
      UNION ALL
      select count() as cnt, 'num_valid_paper_title' as name
      from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn any
               inner join (select pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'GS') using pm_id
      where length(clean_title) > 0
      UNION ALL
      select count() as cnt, 'num_valid_mesh_heading' as name
      from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn any
               inner join (select pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'GS') using pm_id
      where length(clean_mesh_headings) > 0
      UNION ALL
      select count() as cnt, 'num_valid_keyword' as name
      from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn any
               inner join (select pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'GS') using pm_id
      where length(clean_keywords) > 0
      UNION ALL
      select count() as cnt, 'num_valid_aminer_co_author' as name
      from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn any
               inner join (select pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'GS') using pm_id
      where length(arrayFilter(x->length(x) > 0, aminer_co_author_id)) > 0
      UNION ALL
      select count() as cnt, 'num_valid_s2_co_author' as name
      from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn any
               inner join (select pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'GS') using pm_id
      where length(arrayFilter(x->length(x) > 0, s2_co_author_id)) > 0
      UNION ALL
      select count() as cnt, 'num_valid_mag_co_author' as name
      from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn any
               inner join (select pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'GS') using pm_id
      where length(arrayFilter(x->x > 0, mag_co_author_id)) > 0
      UNION ALL
      select count() as cnt, 'num_valid_pkg_v1_co_author' as name
      from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn any
               inner join (select pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'GS') using pm_id
      where length(arrayFilter(x->length(x) > 0, pkg_v1_co_author_id)) > 0
      UNION ALL
      select count() as cnt, 'num_valid_pkg_v2_co_author' as name
      from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn any
               inner join (select pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'GS') using pm_id
      where length(arrayFilter(x->length(x) > 0, pkg_co_author_id)) > 0
      UNION ALL
      select count() as cnt, 'num_valid_vetel_co_author' as name
      from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn any
               inner join (select pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'GS') using pm_id
      where length(arrayFilter(x->length(x) > 0, vetle_co_author_id)) > 0
      UNION ALL
      select count() as cnt, 'num_valid_completed_name' as name
      from and.nft_paper_author_and_dataset_with_ns_id_with_co_author_with_fn any
               inner join (select pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'GS') using pm_id
      where length(one_author[2]) > 0
        and length(one_author[3]) > 1
      UNION ALL
      select sum(length(arrayFilter(x->length(x[2]) > 0 and length(x[3]) > 1, author_list))) as cnt,
             'num_valid_original_name'                                                       as name
      from and.nft_paper_author_name_list any
               inner join (select pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'GS') using pm_id
      UNION ALL
      select sum(length(arrayFilter(x->position(x, '@') > 0, authors_list_raw))) as cnt,
             'num_valid_email'                                                   as name
      from and.nft_paper_author_name_list any
               inner join (select pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'GS') using pm_id
      UNION ALL
      select sum(length(arrayFilter(x->length(JSONExtractString(x, 'aff')) > 0, authors_list_raw))) as cnt,
             'num_valid_aff'                                                                        as name
      from and.nft_paper_author_name_list any
               inner join (select pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'GS') using pm_id
      UNION ALL
      select sum(JSONLength(cast(authors, 'String'), 'authorList') * (abstract_str is NULL ? 0:1)) as cnt,
             'num_valid_has_abstract'                                                              as name
      from pubmed.nft_paper any
               inner join (select toUInt64OrZero(pm_id) as pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'GS')
                          using pm_id
      UNION ALL
      select sum(JSONLength(cast(authors, 'String'), 'authorList') * (references == 'null' ? 0:1)) as cnt,
             'num_valid_has_reference'                                                             as name
      from pubmed.nft_paper any
               inner join (select toUInt64OrZero(pm_id) as pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'GS')
                          using pm_id
      UNION ALL
      select sum(JSONLength(cast(authors, 'String'), 'authorList') * (keywords == 'null' ? 0:1)) as cnt,
             'num_valid_has_keyword'                                                             as name
      from pubmed.nft_paper any
               inner join (select toUInt64OrZero(pm_id) as pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'GS')
                          using pm_id
      UNION ALL
      select sum(JSONLength(cast(authors, 'String'), 'authorList') *
                 (arrayCount(x->x != '"eng"', JSONExtractArrayRaw(cast(languages, 'String'))) == 0 ? 0:1)) as cnt,
             'num_valid_has_lang'                                                                          as name
      from pubmed.nft_paper any
               inner join (select toUInt64OrZero(pm_id) as pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'GS')
                          using pm_id
           -- journal
      UNION ALL
      select sum(JSONLength(cast(authors, 'String'), 'authorList') *
                 (length(JSONExtractString(cast(journal, 'String'), 'title')) == 0 ? 0:1)) as cnt,
             'num_valid_has_journal_title'                                                 as name
      from pubmed.nft_paper any
               inner join (select toUInt64OrZero(pm_id) as pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'GS')
                          using pm_id
           -- co_authors > 0
      UNION ALL
      select sum(JSONLength(cast(authors, 'String'), 'authorList') *
                 (JSONLength(cast(authors, 'String'), 'authorList') <= 1 ? 0:1)) as cnt,
             'num_valid_coauthor_gt1'                                            as name
      from pubmed.nft_paper any
               inner join (select toUInt64OrZero(pm_id) as pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'GS')
                          using pm_id
           -- vernacular_title
      UNION ALL
      select sum(JSONLength(cast(authors, 'String'), 'authorList') *
                 (vernacular_title is null ? 0:1)) as cnt,
             'num_valid_has_vernacular_title'      as name
      from pubmed.nft_paper any
               inner join (select toUInt64OrZero(pm_id) as pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'GS')
                          using pm_id
           -- databank_list
      UNION ALL
      select sum(JSONLength(cast(authors, 'String'), 'authorList') *
                 (data_banks == 'null' ? 0:1)) as cnt,
             'num_valid_has_databank'          as name
      from pubmed.nft_paper any
               inner join (select toUInt64OrZero(pm_id) as pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'GS')
                          using pm_id
           -- grant
      UNION ALL
      select sum(JSONLength(cast(authors, 'String'), 'authorList') *
                 (grants == 'null' ? 0:1)) as cnt,
             'num_valid_has_grant'         as name
      from pubmed.nft_paper any
               inner join (select toUInt64OrZero(pm_id) as pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'GS')
                          using pm_id
           -- pub_type_list
      UNION ALL
      select sum(JSONLength(cast(authors, 'String'), 'authorList') *
                 (JSONLength(cast(publication_types, 'String'), 'publicationTypeList') == 0 ? 0:1)) as cnt,
             'num_valid_has_pub_type'                                                               as name
      from pubmed.nft_paper any
               inner join (select toUInt64OrZero(pm_id) as pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'GS')
                          using pm_id
           -- datetime_str
      UNION ALL
      select sum(JSONLength(cast(authors, 'String'), 'authorList') *
                 (length(datetime_str) == 0 ? 0:1)) as cnt,
             'num_valid_has_pub_date'               as name
      from pubmed.nft_paper any
               inner join (select toUInt64OrZero(pm_id) as pm_id from and.SONG_GS_unique_pm_id where sources[1] = 'GS')
                          using pm_id
         );

select *
from and.GS_dataset_metadata_sparsity;

-- with '{"completeValid":true,"authorList":[{"valid":true,"lastName":"boccardi","foreName":"v","initials":"v"},{"valid":true,"lastName":"fontana","foreName":"l","initials":"l"},{"valid":true,"lastName":"gandolfo","foreName":"g m","initials":"gm"},{"valid":true,"lastName":"grasso","foreName":"a","initials":"a"}]}' as authors
-- select authors, sum(JSONLength(cast(authors, 'String'), 'authorList')) as cnt;

select tupleElement(process_item, 1) as name,
       tupleElement(process_item, 2) as sparsity_pctg
from (
      select arrayJoin(arrayMap(
              i-> (tupleElement(selected_items[i], 1),
                   round(100.0 * tupleElement(selected_items[i], 2) / num_all_authors, 5)),
              range(1, toUInt32(length(selected_items))))) as process_item
      from (select tupleElement(arrayFilter(x->tupleElement(x, 1) == 'num_all_author', groupArray(item) as items)[1],
                                2)                                               as num_all_authors,
                   arrayFilter(x->tupleElement(x, 1) != 'num_all_author', items) as selected_items
            from (
--                   select (name, cnt) as item from and.pubmed_metadata_sparsity
--             select (name, cnt) as item from and.GS_dataset_metadata_sparsity -- running in 19 minutes
                  select (name, cnt) as item from and.SONG_dataset_metadata_sparsity -- running in * minutes
                     ))
         );

select count() as cnt
from (
         select pm_ao, author_id as author_id_x from and.pubmed_and_author_id_split_1)
         any
         inner join (
    select pm_ao, author_id as author_id_y from and.pubmed_and_author_id_1022_1300) using pm_ao
where author_id_x == author_id_y;

select count()
from and.lastname_firat_initial_commonness
where commonness >= 1000
;

-- 172209563
select lowerUTF8(substring(title, 1, 1)) as c, title
from aminer.paper
-- where c <  'a' and c >  'z'
order by lower(substring(title, 1, 1)) desc
limit 500
;

select DisplayName
from aminer.venue
order by lower(substring(DisplayName, 1, 1)) desc
;

select name
from aminer.author
order by lower(substring(name, 1, 1)) desc
limit 500;

select count()
from aminer.author
where position(name, '张') > 0;
;

-- drop table and.AggAND_test_set_result_for_error_analysis;
create table if not exists and.AggAND_test_set_result_for_error_analysis
(
    pm_ao1      String,
    pm_ao2      String,
    same_author Int32,
    pred_prob   Float32,
    time_stamp  Int64
) ENGINE = Log;

select count()
from and.AggAND_test_set_result_for_error_analysis;

create view if not exists and.AggAND_test_set_error_cases as
select *, pred_same_author == 0 ? 'false_negative' : 'false_positive' as error_type
from (
      select *, pred_prob > 0.5 ? 1 : 0 as pred_same_author
      from and.AggAND_test_set_result_for_error_analysis
      where same_author != pred_same_author)
order by error_type
;

-- select *
-- from (select pm_ao1,
--              pm_ao2,
--              same_author,
--              [(lastname1, firstname1, initial1), (outer_lastname1, outer_firstname1, outer_initials1)] as name1,
--              [(lastname2, firstname2, initial2), (outer_lastname2, outer_firstname2, outer_initials2)] as name2,
--              [cased_affiliation1,cased_affiliation2]                                                   as aff_str,
--              [datetime_str1, datetime_str2]                                                            as pub_date,
--              [jd1, jd2]                                                                                as jd,
--              [st1, st2]                                                                                as st,
--              [commonness1, commonness2]                                                                as commonness,
--              [ambiguity_score1, ambiguity_score2]                                                      as ambiguity_score,
--              [extract_keywords1,extract_keywords2]                                                     as extracted_keywords,
--              [same_mag_author_id,same_s2_author_id, same_pkg_aid_v2_author_id]                         as same_aid,
--              [num_mag_coauthors, num_s2_coauthors, num_pkg_v2_coauthors]                               as num_co_authors,
--              [journal_title1, journal_title2]                                                          as journal,
--              [clean_title1, clean_title2]                                                              as title,
--              [clean_abstract1, clean_abstract2]                                                        as abstract,
--              [clean_mesh_headings1, clean_mesh_headings2]                                              as mesh,
--              [clean_keywords1, clean_keywords2]                                                        as keywords,
--              [co_authors_by_name1, co_authors_by_name2]                                                as coauthor,
--              [languages1, languages2]                                                                  as lang
--       from and.SONG_GS_WHU_mix_dataset_pubmed_inner_outer_feature) any
--          inner join and.AggAND_test_set_error_cases using (pm_ao1, pm_ao2)
-- order by error_type;

-- same_pkg_aid_v2_author_id	last name ambiguity score	full name jaccard	same_mag_author_id	date_based_features_0	Journal descriptors
select pm_ao1,
       pm_ao2,
       same_author,
       pred_prob,
       error_type,
       [pkg_aid_v21, pkg_aid_v22, toString(same_pkg_aid_v2_author_id)],
       (ambiguity_score1 + ambiguity_score2) / 2                                  as ambiguity_score,
       [(outer_firstname1, outer_lastname1), (outer_firstname2, outer_lastname2)] as name,
       [mag_author_id1, mag_author_id2, same_mag_author_id],
       [datetime_str1, datetime_str2]                                             as pub_date,
       [jd1, jd2]                                                                 as jd
from (select *
      from and.SONG_GS_WHU_mix_dataset_pubmed_inner_outer_feature any
               inner join (select * from and.AggAND_test_set_error_cases) using (pm_ao1, pm_ao2)) any
         inner join and.GS_dataset_rich_features using (pm_ao1, pm_ao2)
order by error_type;

