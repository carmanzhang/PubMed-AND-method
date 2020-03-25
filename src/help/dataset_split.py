

def check_group_split(X, indx_split):
    for train_index, test_index in indx_split:
        train_X = X[train_index]
        test_X = X[test_index]
        train_side = [n for n in train_X]
        test_side = [n for n in test_X]
        intersection = set(train_side).intersection(set(test_side))
        # print(len(intersection), intersection)
        assert len(intersection) == 0
