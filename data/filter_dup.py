#coding=utf8

def filter_dup(inpath, outpath):
    words = {}
    f = open(inpath, 'rb')
    for line in f:
        atom = line.split('/')
        if len(atom) < 2 or len(atom) > 4:
            print '/'.join(atom)
            continue
        words[atom[0]] = atom[1:]
    f.close()

    f = open(outpath, 'wb')
    for k in words:
        f.write('%s/%s' % (k, '/'.join(words[k])))
    f.close()


if __name__== '__main__':
    #filter_dup(r'./lexicon/lex-company.lex', r'./lexicon/lex-company.lex_')
    filter_dup(r'./lexicon/lex-main.lex', r'./lexicon/lex-main.lex_')
               
               
