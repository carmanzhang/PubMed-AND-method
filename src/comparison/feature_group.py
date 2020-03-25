# • The first author name (first initial and last name)
# • Coauthor list
# • Organization, location, and e-mail detected from affiliation by NER
# • Keywords extracted from paper title and journal (or proceeding) title (connected with whitespace) by MAUI
song_features = ['name_based_features_1', 'name_based_features_4',
                 'coauthor_based_features_0',
                 'affiliation_based_features_5', 'affiliation_based_features_6', 'email_based_features_1',
                 'content_based_features_4']

# Journal descriptors, Semantic types
# Type of organization, Affiliation, Country, City
# Email
# Last name length, First name, Initials, Ambiguity score
# Co-authors
# Years’ difference
# Language
gs_features = ['journal_based_features_1', 'journal_based_features_3',
               'affiliation_based_features_2', 'affiliation_based_features_7',
               'affiliation_based_features_3', 'affiliation_based_features_4',
               'email_based_features_1',
               'name_based_features_3', 'name_based_features_4', 'name_based_features_5', 'name_based_features_9',
               'coauthor_based_features_0',
               'date_based_features_0',
               'language_based_features_1']

# song_gs_combine_features = list(set(gs_features + song_features))
inner_features = gs_features


our_suppl_features = [
    'name_based_features_6',  # our edit distance in char level
    'name_based_features_7',  # our name inclusion
    'journal_based_features_4',  # our journal common words
    'content_based_features_0',  # our Mesh heading
    'vernacular_based_features_0',  # our vernacular title
    'grant_based_features_0',  # our grant agency
    'grant_based_features_1',  # our grant country
    'pubtype_based_features_0',  # our publication type
]

song_gs_combine_and_our_supplement_features = list(set(song_features + gs_features + our_suppl_features))

inner_name_features = [
    'name_based_features_1',  # full name jaccard
    'name_based_features_3',  # last name length
    'name_based_features_7',  # full name name inclusion
    'name_based_features_9',  # last name ambiguity score
]

outer_name_features = [
    'outer_name_based_features_1',  # full name jaccard
    'outer_name_based_features_3',  # last name length
    'outer_name_based_features_7',  # full name name inclusion
    'name_based_features_9',  # last name ambiguity score
]

outer_mag_features = ['same_mag_author_id', 'num_mag_coauthors']
outer_s2_features = ['same_s2_author_id', 'num_s2_coauthors']
outer_pkg_features = ['same_pkg_aid_v2_author_id', 'num_pkg_v2_coauthors']
outer_mag_s2_pkg_features = outer_mag_features + outer_s2_features + outer_pkg_features

outername_inner_features = list(set(outer_name_features + inner_features))
outername_inner_outermags2pkg_features = list(
    set(outer_name_features + inner_features + outer_mag_s2_pkg_features))
