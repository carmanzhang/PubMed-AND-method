import math

import pandas as pd

from eutilities.string_utils import edit_distinct_diff_chars, jaccard_similarity, intersection, extract_email, \
    jaro_winkler_similarity, extract_geo
from model.affiliation import Affiliation
from src.io.data_reader import DBReader

sql = "select * from and.SONG_GS_WHU_mix_dataset_pubmed_inner_outer_feature;"
df = DBReader.cached_read("cached/SONG_GS_WHU_mix_dataset_pubmed_inner_outer_feature.pkl", sql, cached=True)
columns = df.columns.values
print(len(columns), columns)


def extract_name_based_feature(lastname1, firstname1, initial1,
                               lastname2, firstname2, initial2, ):
    full_name_edit_diff_chars = edit_distinct_diff_chars(lastname1 + firstname1, lastname2 + firstname2)

    n1_uniq_chars, n2_uniq_chars = set(lastname1 + firstname1 + initial1), set(lastname2 + firstname2 + initial2)
    diff1 = n1_uniq_chars.difference(n2_uniq_chars)
    diff2 = n2_uniq_chars.difference(n1_uniq_chars)
    num_uniq_char = min(len(diff1), len(diff2))

    # 两个名字的缩写越长，并且差别越小则越可能是同一个作者
    initial_len_score = len(initial1) + len(initial2) - abs(len(initial1) - len(initial2))
    return [jaccard_similarity(list(lastname1 + initial1), list(lastname2 + initial2)),
            jaccard_similarity(list(lastname1 + firstname1), list(lastname2 + firstname2)),
            jaro_winkler_similarity(lastname1 + firstname1, lastname2 + firstname2),  # SONG firstname
            (len(lastname1) + len(lastname2)) / 2.0,  # GS Last name length
            jaro_winkler_similarity(firstname1, firstname2),  # GS First name #4
            1 if initial1 == initial2 else 0,  # GS Initials
            len(full_name_edit_diff_chars),  # our char diff
            num_uniq_char,  # our excluded diff
            initial_len_score]  # 8


def get_location_score(location1: list, location2: list):
    if location1 is None or len(location1) == 0 or location2 is None or len(location2) == 0:
        return 0

    score = 0
    for info1 in location1:
        for info2 in location2:
            if info1.lower().strip() == info2.lower().strip():
                score = score + 1
    return score / ((len(location1) + len(location2)) / 2)


def get_org_type_descr_score(org_string1, org_string2):
    """Returns the cosine between the articles vectors [org_desc, org_type]"""
    t1, d1 = Affiliation.find_type(org_string1), Affiliation.find_descriptor(org_string1)
    t2, d2 = Affiliation.find_type(org_string2), Affiliation.find_descriptor(org_string2)

    if t1 and t2 and d1 and d2:
        num = (d1.value * d2.value) + (t1.value * t2.value)
        denum = math.sqrt((d1.value ** 2 + t1.value ** 2) + (d2.value ** 2 + t2.value ** 2))
        return num / denum
    return 0


def extract_affiliation_based_feature(aff1, cased_aff1, ner_orgs1, ner_locs1,
                                      aff2, cased_aff2, ner_orgs2, ner_locs2):
    # (locs1, orgs1), (locs2, orgs2) = ner(aff1), ner(aff2)
    geo1, geo2 = extract_geo(cased_aff1), extract_geo(cased_aff2)

    country_region_list1, city_list1 = geo1[0] + geo1[1], geo1[2]
    country_region_list2, city_list2 = geo2[0] + geo2[1], geo2[2]

    return [intersection(set(ner_orgs1), set(ner_orgs2)),
            intersection(set(ner_locs1), set(ner_locs2)),
            get_org_type_descr_score(cased_aff1, cased_aff2),  # GS, Type of organization
            intersection(set(country_region_list1), set(country_region_list2)),  # GS, Country
            intersection(set(city_list1), set(city_list2)),  # 4 # GS, City
            jaccard_similarity(' '.join(ner_orgs1).split(' '), ' '.join(ner_orgs2).split(' '),
                               remove_stop_word=True),  # SONG, Organization GS, Organization
            jaccard_similarity(ner_locs1, ner_locs2),  # SONG, Location
            jaro_winkler_similarity(aff1, aff2), # GS Affiliation
            jaro_winkler_similarity(' '.join(ner_orgs1), ' '.join(ner_orgs2))
            ]


def extract_mapaffi_based_feature(affiliation_ids1, departments1, institutions1, emails1, zipcodes1, locations1,
                                  countrys1, citys1, states1, mcountrys1, types1,
                                  affiliation_ids2, departments2, institutions2, emails2, zipcodes2, locations2,
                                  countrys2, citys2, states2, mcountrys2, types2):
    return [
        intersection(affiliation_ids1, affiliation_ids2),
        jaccard_similarity(' '.join(departments1).split(' '), ' '.join(departments2).split(' ')),
        jaccard_similarity(' '.join(institutions1).split(' '), ' '.join(institutions2).split(' ')),
        intersection(emails1, emails2),
        intersection(zipcodes1, zipcodes2),  # 4
        jaccard_similarity(' '.join(locations1).split(' '), ' '.join(locations2).split(' ')),
        intersection(countrys1, countrys2),
        intersection(citys1, citys2),
        intersection(states1, states2),
        intersection(mcountrys1, mcountrys2),  # 9
        intersection(types1, types2),
    ]


def get_organization_score(organization1: list, organization2: list):
    if organization1 is None or len(organization1) == 0 or organization2 is None or len(organization2) == 0:
        return 0
    score = 0
    for info1 in organization1:
        for info2 in organization2:
            if info1 == info2:
                score = score + 1
    return score / ((len(organization1) + len(organization2)) / 2)


def extract_email_based_feature(aff1, aff2):
    email1 = extract_email(aff1)
    email1 = email1 if email1 is not None else ''
    email2 = extract_email(aff2)
    email2 = email2 if email2 is not None else ''
    same_email = 1 if email1 is not None and len(email1) > 0 and email1 == email2 else 0  # GS, Email
    return [same_email,
            jaccard_similarity(list(email1), list(email2)),  # SONG, Email
            jaro_winkler_similarity(email1, email2)]


def extract_journal_based_feature(journal_id1, journal_title1, jd1: list, st1: list, journal_id2, journal_title2,
                                  jd2: list, st2: list):
    # add JD and ST
    return [jaccard_similarity(jd1, jd2),
            intersection(jd1, jd2),  # GS JD
            jaccard_similarity(st1, st2),
            intersection(st1, st2),  # GS ST
            jaccard_similarity(journal_title1.split(' '), journal_title2.split(' '), remove_stop_word=True),
            1 if journal_id1 == journal_id2 else 0]


def extract_content_based_feature(title1, abstract1, mesh_headings1, keywords1, extract_keywords1, journal_title1,
                                  title2, abstract2, mesh_headings2, keywords2, extract_keywords2, journal_title2):
    title_abstract1 = title1 + ' ' + abstract1
    title_abstract2 = title2 + ' ' + abstract2
    # extract_keywords1, extract_keywords2 = ' '.join(extract_keywords1), ' '.join(extract_keywords2)
    inner_keywords1, inner_keywords2 = mesh_headings1 + ' ' + keywords1, mesh_headings2 + ' ' + keywords2
    return [intersection(mesh_headings1.split(' '), mesh_headings2.split(' '), remove_stop_word=True),  # our MeSH terms
            jaccard_similarity((title1 + ' ' + journal_title1).split(' '), (title2 + ' ' + journal_title2).split(' '),
                               remove_stop_word=True),
            jaccard_similarity(title_abstract1.split(' '), title_abstract2.split(' '), remove_stop_word=True),
            jaccard_similarity(inner_keywords1.split(' '), inner_keywords2.split(' '), remove_stop_word=True),
            jaccard_similarity(extract_keywords1, extract_keywords2)  # SONG common extracted keywords
            ]


def extract_language_based_feature(languages1, langs_from_other_abstract1, languages2, langs_from_other_abstract2):
    lang_list1 = set([n for n in languages1 + langs_from_other_abstract1 if n != 'eng'])
    lang_list2 = set([n for n in languages2 + langs_from_other_abstract2 if n != 'eng'])

    return [
        1 if len(languages1) > 0 and len(languages2) > 0 and languages1[0] == languages2[0] else 0,
        intersection(lang_list1, lang_list2)  # GS, Language
    ]


def extract_vernacular_based_feature(vernacular_title1, vernacular_title2):
    both_has_vernacular = len(vernacular_title1) > 0 and len(vernacular_title2) > 0
    return [1 if both_has_vernacular else 0]


def extract_citation_based_feature(cited_pm_id1, cited_pm_id2):
    return [intersection(cited_pm_id1, cited_pm_id2),
            jaccard_similarity(cited_pm_id1, cited_pm_id2)]


def extract_databank_based_feature(databank_list1, databank_list2):
    return [intersection(databank_list1, databank_list2)]


def extract_grant_based_feature(grant_agency_list1, grant_country_list1, grant_agency_list2, grant_country_list2):
    # print(grant_agency_list1, grant_country_list1, grant_agency_list2, grant_country_list2)
    grant_agency_list1 = list(set([n[:n.index(' ')] if ' ' in n else n for n in grant_agency_list1]))
    grant_agency_list2 = list(set([n[:n.index(' ')] if ' ' in n else n for n in grant_agency_list2]))
    grant_country_list1, grant_country_list2 = list(set(grant_country_list1)), list(set(grant_country_list2))

    num_common_agencies, num_common_country = intersection(grant_agency_list1, grant_agency_list2), intersection(
        grant_country_list1, grant_country_list2)
    if num_common_agencies == 0 and len(grant_agency_list1) > 0 and len(grant_agency_list2) > 0:
        num_common_agencies = -1
    if num_common_country == 0 and len(grant_country_list1) > 0 and len(grant_country_list2) > 0:
        num_common_country = -1
    return [num_common_agencies, num_common_country]


def extract_pubtype_based_feature(pub_type_list1, pub_type_ui_list1, pub_type_list2, pub_type_ui_list2):
    return [jaccard_similarity(pub_type_ui_list1, pub_type_ui_list2)]


def extract_date_based_feature(datetime_str1, datetime_str2):
    year_diff = abs(int(datetime_str1[:4]) - int(datetime_str2[:4]))
    return [year_diff]  # GS, Years’ difference


def extract_coauthor_based_feature(lastname1, firstname1, initial1, co_authors_by_name1, lastname2, firstname2,
                                   initial2, co_authors_by_name2):
    current_author1 = lastname1 + '_' + (initial1[0] if len(initial1) > 0 else '')
    current_author2 = lastname2 + '_' + (initial2[0] if len(initial2) > 0 else '')
    co_authors_by_name1.remove(current_author1)
    co_authors_by_name2.remove(current_author2)
    co_authors = set(co_authors_by_name1).intersection(co_authors_by_name2)
    return [len(co_authors)]  # GS Co-authors # SONG Coauthor


all_features = []
feature_names = []
# feature_from = 'inner'
# feature_from = 'outer'
feature_from = 'inner_outer'
for i, row in df.iterrows():
    if i % 1000 == 0:
        print(i, df.shape[0])
    pm_ao1, pm_ao2, same_author, _, \
    pm_id1, author_order1, lastname1, firstname1, initial1, affiliation1, journal_nlmUniqueID1, \
    journal_title1, clean_title1, clean_abstract1, clean_mesh_headings1, clean_keywords1, co_authors_by_name1, languages1, vernacular_title1, cited_pm_id1, \
    databank_list1, grant_agency_list1, grant_country_list1, pub_type_list1, pub_type_ui_list1, langs_from_other_abstract1, datetime_str1, \
    ns1, jd1, st1, cased_affiliation1, ner_orgs1, ner_locs1, \
    affiliation_ids1, departments1, institutions1, emails1, zipcodes1, locations1, countrys1, citys1, states1, mcountrys1, types1, \
    commonness1, ambiguity_score1, shared_lastname, lastname_hash_partition_for_split, \
    pm_id2, author_order2, lastname2, firstname2, initial2, affiliation2, journal_nlmUniqueID2, \
    journal_title2, clean_title2, clean_abstract2, clean_mesh_headings2, clean_keywords2, co_authors_by_name2, languages2, vernacular_title2, cited_pm_id2, \
    databank_list2, grant_agency_list2, grant_country_list2, pub_type_list2, pub_type_ui_list2, langs_from_other_abstract2, datetime_str2, \
    ns2, jd2, st2, cased_affiliation2, ner_orgs2, ner_locs2, \
    affiliation_ids2, departments2, institutions2, emails2, zipcodes2, locations2, countrys2, citys2, states2, mcountrys2, types2, \
    commonness2, ambiguity_score2, \
    extract_keywords1, extract_keywords2, source, \
    outer_lastname1, outer_firstname1, outer_initials1, outer_lastname2, outer_firstname2, outer_initials2, \
    same_author_id, same_mag_author_id, same_s2_author_id, same_vetle_aid_author_id, same_pkg_aid_v1_author_id, same_pkg_aid_v2_author_id, \
    num_mag_coauthors, num_s2_coauthors, num_vetel_coauthors, num_pkg_v1_coauthors, num_pkg_v2_coauthors = row

    name_based_features = extract_name_based_feature(lastname1, firstname1, initial1,
                                                     lastname2, firstname2, initial2)
    name_based_features.append((ambiguity_score1 + ambiguity_score2) / 2)  # GS Ambiguity score
    # outer_firstname1 = firstname1 if len(firstname1.replace(' ', '')) > 2 else outer_firstname1
    # outer_firstname2 = firstname2 if len(firstname2.replace(' ', '')) > 2 else outer_firstname2
    outer_name_based_features = extract_name_based_feature(outer_lastname1, outer_firstname1, outer_initials1,
                                                           outer_lastname2, outer_firstname2, outer_initials2)
    outer_author_id_based_features = [same_author_id, same_mag_author_id, same_s2_author_id, same_vetle_aid_author_id,
                                      same_pkg_aid_v1_author_id, same_pkg_aid_v2_author_id]
    outer_num_coauthor_based_features = [num_mag_coauthors, num_s2_coauthors, num_vetel_coauthors, num_pkg_v1_coauthors,
                                         num_pkg_v2_coauthors]
    affiliation_based_features = extract_affiliation_based_feature(affiliation1, cased_affiliation1, ner_orgs1,
                                                                   ner_locs1,
                                                                   affiliation2, cased_affiliation2, ner_orgs2,
                                                                   ner_locs2)
    mapaffi_based_features = extract_mapaffi_based_feature(affiliation_ids1, departments1, institutions1, emails1,
                                                           zipcodes1, locations1, countrys1, citys1, states1,
                                                           mcountrys1, types1,
                                                           affiliation_ids2, departments2, institutions2, emails2,
                                                           zipcodes2, locations2, countrys2, citys2, states2,
                                                           mcountrys2, types2)
    email_based_features = extract_email_based_feature(affiliation1, affiliation2)
    journal_based_features = extract_journal_based_feature(journal_nlmUniqueID1, journal_title1, jd1, st1,
                                                           journal_nlmUniqueID2, journal_title2, jd2, st2)
    content_based_features = extract_content_based_feature(clean_title1, clean_abstract1, clean_mesh_headings1,
                                                           clean_keywords1, extract_keywords1, journal_title1,
                                                           clean_title2, clean_abstract2, clean_mesh_headings2,
                                                           clean_keywords2, extract_keywords2, journal_title2)
    language_based_features = extract_language_based_feature(languages1, langs_from_other_abstract1, languages2,
                                                             langs_from_other_abstract2)
    coauthor_based_features = extract_coauthor_based_feature(lastname1, firstname1, initial1, co_authors_by_name1,
                                                             lastname2, firstname2, initial2, co_authors_by_name2)
    vernacular_based_features = extract_vernacular_based_feature(vernacular_title1, vernacular_title2)
    citation_based_features = extract_citation_based_feature(cited_pm_id1, cited_pm_id2)
    databank_based_features = extract_databank_based_feature(databank_list1, databank_list2)
    grant_based_features = extract_grant_based_feature(grant_agency_list1, grant_country_list1, grant_agency_list2,
                                                       grant_country_list2)
    pubtype_based_features = extract_pubtype_based_feature(pub_type_list1, pub_type_ui_list1, pub_type_list2,
                                                           pub_type_ui_list2)
    date_based_features = extract_date_based_feature(datetime_str1, datetime_str2)
    if feature_from == 'inner':
        all_features.append([pm_ao1, pm_ao2, same_author, source, shared_lastname,
                             lastname_hash_partition_for_split] + name_based_features +
                            affiliation_based_features + mapaffi_based_features + email_based_features +
                            journal_based_features + content_based_features + language_based_features +
                            coauthor_based_features + vernacular_based_features + citation_based_features +
                            databank_based_features + grant_based_features + pubtype_based_features + date_based_features)
        if i == df.shape[0] - 1:
            feature_names = ['pm_ao1', 'pm_ao2', 'same_author', 'source', 'shared_lastname',
                             'lastname_hash_partition_for_split'] + ['name_based_features_' + str(i) for i in
                                                                     range(len(name_based_features))] + [
                                'affiliation_based_features_' + str(i) for i in
                                range(len(affiliation_based_features))] + [
                                'mapaffi_based_features_' + str(i) for i in range(len(mapaffi_based_features))] + [
                                'email_based_features_' + str(i) for i in range(len(email_based_features))] + [
                                'journal_based_features_' + str(i) for i in range(len(journal_based_features))] + [
                                'content_based_features_' + str(i) for i in range(len(content_based_features))] + [
                                'language_based_features_' + str(i) for i in range(len(language_based_features))] + [
                                'coauthor_based_features_' + str(i) for i in range(len(coauthor_based_features))] + [
                                'vernacular_based_features_' + str(i) for i in
                                range(len(vernacular_based_features))] + [
                                'citation_based_features_' + str(i) for i in range(len(citation_based_features))] + [
                                'databank_based_features_' + str(i) for i in range(len(databank_based_features))] + [
                                'grant_based_features_' + str(i) for i in range(len(grant_based_features))] + [
                                'pubtype_based_features_' + str(i) for i in range(len(pubtype_based_features))] + [
                                'date_based_features_' + str(i) for i in range(len(date_based_features))]
    elif feature_from == 'outer':
        all_features.append([pm_ao1, pm_ao2, same_author, source, shared_lastname,
                             lastname_hash_partition_for_split] + outer_name_based_features + outer_author_id_based_features + outer_num_coauthor_based_features)
        if i == df.shape[0] - 1:
            feature_names = ['pm_ao1', 'pm_ao2', 'same_author', 'source', 'shared_lastname',
                             'lastname_hash_partition_for_split'] + [
                                'outer_name_based_features_' + str(i) for i in range(len(outer_name_based_features))] + \
                            ['same_author_id', 'same_mag_author_id', 'same_s2_author_id', 'same_vetle_aid_author_id',
                             'same_pkg_aid_v1_author_id', 'same_pkg_aid_v2_author_id'] + \
                            ['num_mag_coauthors', 'num_s2_coauthors', 'num_vetel_coauthors', 'num_pkg_v1_coauthors',
                             'num_pkg_v2_coauthors']
    else:
        all_features.append([pm_ao1, pm_ao2, same_author, source, shared_lastname, lastname_hash_partition_for_split] +
                            name_based_features + outer_name_based_features + outer_author_id_based_features + outer_num_coauthor_based_features +
                            affiliation_based_features + mapaffi_based_features + email_based_features +
                            journal_based_features + content_based_features + language_based_features +
                            coauthor_based_features + vernacular_based_features + citation_based_features +
                            databank_based_features + grant_based_features + pubtype_based_features + date_based_features)
        if i == df.shape[0] - 1:
            feature_names = ['pm_ao1', 'pm_ao2', 'same_author', 'source', 'shared_lastname',
                             'lastname_hash_partition_for_split'] + [
                                'name_based_features_' + str(i) for i in range(len(name_based_features))] + [
                                'outer_name_based_features_' + str(i) for i in range(len(outer_name_based_features))] + \
                            ['same_author_id', 'same_mag_author_id', 'same_s2_author_id', 'same_vetle_aid_author_id',
                             'same_pkg_aid_v1_author_id', 'same_pkg_aid_v2_author_id'] + \
                            ['num_mag_coauthors', 'num_s2_coauthors', 'num_vetel_coauthors', 'num_pkg_v1_coauthors',
                             'num_pkg_v2_coauthors'] + [
                                'affiliation_based_features_' + str(i) for i in
                                range(len(affiliation_based_features))] + [
                                'mapaffi_based_features_' + str(i) for i in range(len(mapaffi_based_features))] + [
                                'email_based_features_' + str(i) for i in range(len(email_based_features))] + [
                                'journal_based_features_' + str(i) for i in range(len(journal_based_features))] + [
                                'content_based_features_' + str(i) for i in range(len(content_based_features))] + [
                                'language_based_features_' + str(i) for i in range(len(language_based_features))] + [
                                'coauthor_based_features_' + str(i) for i in range(len(coauthor_based_features))] + [
                                'vernacular_based_features_' + str(i) for i in
                                range(len(vernacular_based_features))] + [
                                'citation_based_features_' + str(i) for i in range(len(citation_based_features))] + [
                                'databank_based_features_' + str(i) for i in range(len(databank_based_features))] + [
                                'grant_based_features_' + str(i) for i in range(len(grant_based_features))] + [
                                'pubtype_based_features_' + str(i) for i in range(len(pubtype_based_features))] + [
                                'date_based_features_' + str(i) for i in range(len(date_based_features))]

pd.DataFrame(all_features, columns=feature_names).to_csv('cached/pubmed_%s_feature.tsv' % feature_from, sep='\t',
                                                         index=False)
