#coding=utf8


def get_len(line, sepa=None, widx=0):
    line = line.split(sepa)
    if len(line) < (widx+1):
        return 0  # 无效
    return len(line[widx])

def filter_file(inpath, outpath):
    res = []
    sepa = u'/'
    wcode = 'utf8'
    f = open(inpath, 'rb')
    for line in f:
        try:
            line = line.decode(wcode)
        except Exception, e:
            return -1  # 编码错误
        line = line.split(sepa)
        if len(line[0]) < 2:
            print line
        else:
            res.append(sepa.join(line).encode(wcode))
    f.close()
    f = open(outpath, 'wb')
    f.write(''.join(res))
    f.close()
    return None

def process():
    p1 = [ r'./lex-main.lex',r'./lex-main_.lex',r'./lex-company.lex' ]
    for f in p1:
        filter_file(f, f+'_')
    return



if __name__ == '__main__':
    #process()

    

    
    

