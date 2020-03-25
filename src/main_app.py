from comparison import author_id_baseline, name_feature_method, gs_song_baselines, inner_feature_method, \
    inner_outer_feature_method

if __name__ == '__main__':
    cached_file_path = 'cached/SONG_GS_WHU_mix_dataset_external_author_id.pkl'
    author_id_baseline.run(which_external_id='same_mag_author_id', dataset='GS', cached_file_path=cached_file_path)
    author_id_baseline.run(which_external_id='same_s2_author_id', dataset='GS', cached_file_path=cached_file_path)
    author_id_baseline.run(which_external_id='same_pkg_aid_v2_author_id', dataset='GS',
                           cached_file_path=cached_file_path)
    author_id_baseline.run(which_external_id='same_mag_author_id', dataset='SONG', cached_file_path=cached_file_path)
    author_id_baseline.run(which_external_id='same_s2_author_id', dataset='SONG', cached_file_path=cached_file_path)
    author_id_baseline.run(which_external_id='same_pkg_aid_v2_author_id', dataset='SONG',
                           cached_file_path=cached_file_path)

    cached_file_path = 'cached/pubmed_inner_outer_feature.tsv'
    name_feature_method.run(method='innername_features', dataset='GS', cached_file_path=cached_file_path)
    name_feature_method.run(method='outername_features', dataset='GS', cached_file_path=cached_file_path)
    name_feature_method.run(method='innername_features', dataset='SONG', cached_file_path=cached_file_path)
    name_feature_method.run(method='outername_features', dataset='SONG', cached_file_path=cached_file_path)

    gs_song_baselines.run(method='SONG_feature_set', dataset='GS',
                          cached_file_path=cached_file_path)
    gs_song_baselines.run(method='GS_feature_set', dataset='GS',
                          cached_file_path=cached_file_path)
    gs_song_baselines.run(method='SONG_feature_set', dataset='SONG',
                          cached_file_path=cached_file_path)
    gs_song_baselines.run(method='GS_feature_set', dataset='SONG',
                          cached_file_path=cached_file_path)

    inner_feature_method.run(method='song_gs_combine_feature_set', dataset='GS',
                             cached_file_path=cached_file_path)
    inner_feature_method.run(method='song_gs_combine_and_our_supplement_features', dataset='GS',
                             cached_file_path=cached_file_path)
    inner_feature_method.run(method='song_gs_combine_feature_set', dataset='SONG',
                             cached_file_path=cached_file_path)
    inner_feature_method.run(method='song_gs_combine_and_our_supplement_features', dataset='SONG',
                             cached_file_path=cached_file_path)

    inner_outer_feature_method.run(method='outer_name_features', dataset='GS',
                                   cached_file_path=cached_file_path)
    inner_outer_feature_method.run(method='inner_features', dataset='GS',
                                   cached_file_path=cached_file_path)
    inner_outer_feature_method.run(method='outer_mag_s2_pkg_features', dataset='GS',
                                   cached_file_path=cached_file_path)
    inner_outer_feature_method.run(method='exclude_outer_name_features', dataset='GS',
                                   cached_file_path=cached_file_path)
    inner_outer_feature_method.run(method='exclude_inner_features', dataset='GS',
                                   cached_file_path=cached_file_path)
    inner_outer_feature_method.run(method='exclude_outer_mag_s2_pkg_features', dataset='GS',
                                   cached_file_path=cached_file_path)
    inner_outer_feature_method.run(method='outername_inner_outermags2pkg_features', dataset='GS',
                                   cached_file_path=cached_file_path)

    inner_outer_feature_method.run(method='outer_name_features', dataset='SONG',
                                   cached_file_path=cached_file_path)
    inner_outer_feature_method.run(method='inner_features', dataset='SONG',
                                   cached_file_path=cached_file_path)
    inner_outer_feature_method.run(method='outer_mag_s2_pkg_features', dataset='SONG',
                                   cached_file_path=cached_file_path)
    inner_outer_feature_method.run(method='exclude_outer_name_features', dataset='SONG',
                                   cached_file_path=cached_file_path)
    inner_outer_feature_method.run(method='exclude_inner_features', dataset='SONG',
                                   cached_file_path=cached_file_path)
    inner_outer_feature_method.run(method='exclude_outer_mag_s2_pkg_features', dataset='SONG',
                                   cached_file_path=cached_file_path)
    inner_outer_feature_method.run(method='outername_inner_outermags2pkg_features', dataset='SONG',
                                   cached_file_path=cached_file_path)
