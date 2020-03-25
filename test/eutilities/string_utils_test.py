from Levenshtein.StringMatcher import StringMatcher

str_matcher = StringMatcher()

str1 = 'deceukelaire'
str2 = 'de ceukelairef'

str_matcher.set_seqs(str1, str2)
editops = str_matcher.get_editops()

print(editops)
involved_chars = []
for model, pos1, pos2 in editops:
    if model == 'delete':
        print('delete: ', str1[pos1])
        involved_chars.append(str1[pos1])
    elif model == 'replace':
        print('replace: ', str1[pos1])
        involved_chars.append(str1[pos1])
    elif model == 'insert':
        print('insert: ', str2[pos1])
        involved_chars.append(str2[pos1])

print(involved_chars)

### test spllit
split = 'research support, n.i.h., extramural'.strip('research support, ')
print(split)
index = split.index('e')
print(index)
