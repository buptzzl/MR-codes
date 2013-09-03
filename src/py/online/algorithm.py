#coding=utf8

from time import strptime, mktime
#from math import pow

class online1:
    """ 实时算法的单机实现，用于测试 """
    def __init__(self, ppath):
        """ 固定的参数 """
        self.alpha = 1
        self.beta = -1
        self.gamma = -1
        self.delta = 1
        self.epsilon = 1
        self.zeta = 1
        self.topK = 2
        self.K1 = 2
        self.waction = {'1': 1, '2': 1}  # 行为类型的权重
        
        self.M = [[0] * 3, [0] * 3, [0] * 3]  # 测试数据中的类别数为 3个. 注意浅拷贝的影响
        self.UE = dict()  # uid:[event]
        self.cidx = dict()  # uid:index.
        self.cid_all = []  # (uid).
        #self.__load_parameter(ppath)

    def __load_parameter(self, ppath=None):
        """ TODO 加载参数文件 """
        pass

    def process(self, line):
        """ 传递进来一个用户事件<uid, time, type, classID>， 更新矩阵， 预测结果 """
        ei = self.__parse_data(line)
        if ei is None:
            return None
        uevent = self.get_user_action(ei[0], ei[1:])
        self.update_matrix(uevent)  # 在预测前更新矩阵
        res = self.predict(uevent)
        #del ei
        #del uevent
        return res

    def __parse_data(self, line):
        """ 解析一行用户事件数据. @ret 一个事件list """
        note = '#'
        sepa = ', '
        N = 4
        if line is None or len(line) == 0 or line[0] == note:
            return None

        atom = line.strip().split(sepa)
        if len(atom) != N:
            return None
        atom[1] = mktime(strptime(atom[1], '%Y%m%d%H%M%S'))
        return atom

    def get_user_action(self, uid=None, event=None):
        """ 估计uid 返回有效列表 """
        if uid is None:  # or uid not in self.UE:
            return None
        if event[-1] not in self.cidx:
            self.cidx[event[-1]] = len(self.cidx)
            self.cid_all.append(event[-1])
        if uid not in self.UE:
            self.UE[uid] = [event]
        es = self.UE[uid]
        for ei in es:
            if event == ei or ei[0] > event[0]:  # 有完全相同的事件|时间非升序，不再添加
                return es
        es.append(event)
        #es = sorted(es, key = lambda x:x[0])  # sort by time.
        return es

    def update_matrix(self, uevents):
        """ 更新某个用户产生的最新事件序列到矩阵 """
        if uevents is None or len(uevents) == 0:
            return None
        idx = self.cidx[uevents[-1][-1]]
        for ei in uevents:  # 对角线上元素即该cid的全局频率
            edx = self.cidx[ei[-1]]
            if idx < edx:  # 仅填充上半三角区域
                self.M[idx][edx] += 1
            else:
                self.M[edx][idx] += 1
        return None

    def predict(self, uevents):
        """ 预测出用户的推荐列表. 无结果是返回[] """
        if uevents is None or len(uevents) == 0:
            return []
        #res = [[-1, 0.0] * self.topK]  # 加载[cid, weight]
        res = []
        for i in xrange(self.topK):
            res.append([-1, 0.0])
        N = len(uevents)
        idx = self.cidx[uevents[-1][-1]]
        p_ave = self.average(idx)
        uids = self.user_action(uevents)
        for i in xrange(N-1, -1, -1):
            #cpair = [-1, 0.0]
            ei = uevents[i]
            edx = self.cidx[ei[2]]            
            sum_f = uids[edx]            
            if sum_f == 0:  # 不重复计算
                continue
            else:
                uids[edx] = 0
            
            Dt = uevents[-1][0] - ei[0]
            Npath = N - i
            L = 1  # 固定的层次
            factor = pow(Dt == 0 and 1 or Dt, self.beta) * pow(Npath == 0 and 1 or Npath, self.gamma)\
                       * pow(L == 0 and 1 or L, self.delta) * pow(sum_f == 0 and 1 or sum_f, self.epsilon)
            for mi in xrange(0, edx):
                si = self.M[mi][edx] / p_ave
                si = factor * pow(si, self.alpha)
                if si > res[0][1]:
                    #cpair = [mi, si]
                    self.update_rank(res, mi, si)  
            for mi in xrange(edx, len(self.M[edx])):
                si = self.M[edx][mi] / p_ave
                si = factor * pow(si, self.alpha)
                if si > res[0][1]:
                    self.update_rank(res, mi, si)            
            
            #cpir[0] = self.cid_all[cpir[0]]  # 还原为cid
            #cpair[1] = cpair[1] 
            #res.append(tuple(cpair))
        for ai in res:
            ai[0] = self.cid_all[ai[0]]  # 还原为 cid
        res = sorted(res, key=lambda x: x[1])
        if uevents[-1][-1] not in set([ai[0] for ai in res]):
            w_now = res[self.K1 - 1][1]
            #print '[Test] ', res, '\t', self.K1
            res.append([uevents[-1][-1], w_now])  # 添加当前ID

        return res

    def update_rank(self, rlist, idIdx, score):
        """ 将[uid, score]插入到升序排列的 rlist, 最小元素出局 """
        if rlist is None or len(rlist) == 0:
            return None  # rlist
        i = 0;
        for i in xrange(len(rlist)):
            if rlist[i][1] >= score:
                break;
        j = 0;
        for j in xrange(1, i):
            rlist[j-1] = rlist[j]
        rlist[j] = [idIdx, score]
        return None  # rlist
    
    def average(self, idx):
        """ 得到索引下的 平均分值 """
        N = float(len([ai for ai in self.M[idx] if ai != 0]))
        return sum(self.M[idx]) / N

    def user_action(self, events):
        """ 基于用户序列按时间逆序统计频率 @ret {cidx:F} """
        if events is None or len(events) == 0:
            return None
        actions = dict()
        for i in xrange(len(events)):
            idx = self.cidx[events[i][2]]
            if idx not in actions:
                actions[idx] = self.waction[events[i][1]]
            else:
                actions[idx] += self.waction[events[i][1]]
        return actions

    
if __name__  == '__main__':
    ua = online1(None)
    f = open(r'D:/Data/MR-codes/src/py/online/events-time.txt', 'rb')
    for line in f:
        rank = ua.process(line)
        print rank
    f.close()
