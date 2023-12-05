import threading
from queue import Queue
import hashlib
import random
import lz4.block

#D:\python38\python.exe D:/学习资料/deepsketch/deepsketch-py/clustering/coarse_xdelta3.py D:/学习资料/discuss.txt 2

BLOCK_SIZE = 4096
INF = 987654321
COARSE_T = 2048
MAX_THREAD = 256

N = 0
NUM_THREAD = 0
trace = []
buf = [[''] * BLOCK_SIZE for _ in range(MAX_THREAD)]
compressed = [[''] * (2 * BLOCK_SIZE) for _ in range(MAX_THREAD)]


def do_lz4(i, j, id):
    #print('#')
    #print(lz4.block.compress(trace[i]))
    return len(lz4.block.compress(trace[i]))


class BFinfo(tuple):
    def __new__(cls, i, j, size, ref_index):
        return super(BFinfo, cls).__new__(cls, (i, j, size, ref_index))

    def __lt__(self, other):
        return self[0] < other[0]


class BruteForceClusterArgument:
    def __init__(self, mutex, id, todo, rep_list, rep_cnt, produceQ, resultQ, do_compress):
        self.mutex = mutex
        self.id = id
        self.todo = todo
        self.rep_list = rep_list
        self.rep_cnt = rep_cnt
        self.produceQ = produceQ
        self.resultQ = resultQ
        self.do_compress = do_compress


def BF(arg):
    while True:
        with arg.mutex:
            if arg.produceQ.empty():
                continue
            i = arg.produceQ.get()
            if i == INF:
                return
            mx = arg.rep_cnt[0]

        compress_min = INF
        ref_index = -1
        for j in range(mx):
            now = arg.do_compress(arg.todo[i], arg.rep_list[j], arg.id)
            if now < compress_min:
                compress_min = now
                ref_index = j

        with arg.mutex:
            arg.resultQ.put(BFinfo(i, mx, compress_min, ref_index))


def bruteForceCluster(todo, cluster, threshold):
    MAX_QSIZE = 2 * NUM_THREAD
    mutex = threading.Lock()
    rep_list = cluster + todo
    rep_cnt = len(cluster)

    # Priority Queue: {index in todo, checked index of rep_list, size, ref index of rep_list}
    produceQ = Queue()
    resultQ = Queue()

    for i in range(min(MAX_QSIZE, len(todo))):
        produceQ.put(i)

    arg = BruteForceClusterArgument(mutex, NUM_THREAD - 1, todo, rep_list, [rep_cnt], produceQ, resultQ, do_lz4)

    # Create thread
    threads = [threading.Thread(target=BF, args=(arg,)) for _ in range(NUM_THREAD - 1)]
    for t in threads:
        t.start()

    for i in range(len(todo)):
        print(len(todo))
        print(i)
        now = resultQ.get()

        compress_min = now[2]
        ref_index = now[3]

        for j in range(now[1], len(rep_list)):
            e = arg.do_compress(todo[i], rep_list[j], NUM_THREAD - 1)
            if e < int(compress_min):
                compress_min = e
                ref_index = j

        #print(todo[i])
        if len(cluster)==0:
            cluster.append([])
        if compress_min <= threshold:
            if 0 <= ref_index <= len(cluster):
                cluster[ref_index].append(todo[i])
        else:
            cluster.append([todo[i]])
            with mutex:
                rep_list.append(todo[i])
                rep_cnt += 1

        if i + MAX_QSIZE < len(todo):
            with mutex:
                produceQ.put(i + MAX_QSIZE)

        if i % 1000 == 999:
            print(f"{i}, qsize: {resultQ.qsize()}, rep_cnt: {rep_cnt}")

    with mutex:
        produceQ.put(INF)

    for t in threads:
        t.join()


def print_cluster(cluster):
    for c in cluster:
        print(len(c), end=" ")
        print(*c)
    print()

def write_cluster(cluster, output_file):
    with open(output_file, 'wt') as f:
        for c in cluster:
            f.write(f"{len(c)} ")
            f.write(" ".join(map(str, c)))
            f.write("\n")
        f.write("\n")

def read_file(name):
    global N
    N = 0
    trace.clear()

    with open(name, 'rb') as f:
        while True:
            data = f.read(BLOCK_SIZE)
            #print(data)
            if not data:
                break
            trace.append(data)
            N += 1


def main():
    global NUM_THREAD

    import sys

    if len(sys.argv) != 3:
        print("usage: ./coarse [input_file] [num_thread]")
        sys.exit(0)

    NUM_THREAD = int(sys.argv[2])

    read_file(sys.argv[1])

    dedup = set()
    unique_list = []
    for i in range(N):
        h = hashlib.md5(trace[i]).hexdigest()

        if h in dedup:
            continue
        else:
            dedup.add(h)
            unique_list.append(i)

    cluster = []
    #print(trace)
    bruteForceCluster(unique_list, cluster, COARSE_T)

    unique_list.clear()
    newcluster = []
    #print(len(cluster))
    for i in range(len(cluster)):
        lz4_min = INF
        rep = -1
        for j in cluster[i]:
            r = random.random()
            if r < (len(cluster[i]) - 1000) / len(cluster[i]):
                continue

            s = sum(do_lz4(j, k, 0) for k in cluster[i])

            if s < lz4_min:
                lz4_min = s
                rep = j

        newcluster.append([rep])
        for j in cluster[i]:
            if j != rep:
                unique_list.append(j)

    bruteForceCluster(unique_list, newcluster, COARSE_T)
    print_cluster(newcluster)
    write_cluster(newcluster,f"D:/学习资料/deepsketch/deepsketch-py/clustering/result/mix_coarse")


if __name__ == "__main__":
    main()







# import threading
# from queue import Queue
# import hashlib
# import random
# import lz4.block
# import xdelta3
#
# BLOCK_SIZE = 4096
# INF = 987654321
# COARSE_T = 2048
# MAX_THREAD = 256
#
# N = 0
# NUM_THREAD = 0
# trace = []
# buf = [[''] * BLOCK_SIZE for _ in range(MAX_THREAD)]
# compressed = [[''] * (2 * BLOCK_SIZE) for _ in range(MAX_THREAD)]
#
#
# def do_xdelta3(i, j, id):
#     return len(xdelta3.encode(trace[i], trace[j]))
#
#
# class BFinfo(tuple):
#     def __new__(cls, i, j, size, ref_index):
#         return super(BFinfo, cls).__new__(cls, (i, j, size, ref_index))
#
#     def __lt__(self, other):
#         return self[0] < other[0]
#
#
# class BruteForceClusterArgument:
#     def __init__(self, mutex, id, todo, rep_list, rep_cnt, produceQ, resultQ):
#         self.mutex = mutex
#         self.id = id
#         self.todo = todo
#         self.rep_list = rep_list
#         self.rep_cnt = rep_cnt
#         self.produceQ = produceQ
#         self.resultQ = resultQ
#
#
# def BF(arg):
#     while True:
#         with arg.mutex:
#             if arg.produceQ.empty():
#                 continue
#             i = arg.produceQ.get()
#             if i == INF:
#                 return
#             mx = arg.rep_cnt[0]
#
#         compress_min = INF
#         ref_index = -1
#         for j in range(mx):
#             now = do_xdelta3(arg.todo[i], arg.rep_list[j], arg.id)
#             if now < compress_min:
#                 compress_min = now
#                 ref_index = j
#
#         with arg.mutex:
#             arg.resultQ.put(BFinfo(i, mx, compress_min, ref_index))
#
#
# def bruteForceCluster(todo, cluster, threshold):
#     MAX_QSIZE = 2 * NUM_THREAD
#     mutex = threading.Lock()
#     rep_list = cluster + todo
#     rep_cnt = len(cluster)
#
#     # Priority Queue: {index in todo, checked index of rep_list, size, ref index of rep_list}
#     produceQ = Queue()
#     resultQ = Queue()
#
#     for i in range(min(MAX_QSIZE, len(todo))):
#         produceQ.put(i)
#
#     arg = BruteForceClusterArgument(mutex, NUM_THREAD - 1, todo, rep_list, [rep_cnt], produceQ, resultQ)
#
#     # Create thread
#     threads = [threading.Thread(target=BF, args=(arg,)) for _ in range(NUM_THREAD - 1)]
#     for t in threads:
#         t.start()
#
#     for i in range(len(todo)):
#         now = resultQ.get()
#
#         compress_min = now[2]
#         ref_index = now[3]
#
#         for j in range(now[1], len(rep_list)):
#             e = do_xdelta3(todo[i], rep_list[j], NUM_THREAD - 1)
#             if e < int(compress_min):
#                 compress_min = e
#                 ref_index = j
#
#         if compress_min <= threshold:
#             if ref_index >= 0 and ref_index < len(cluster):
#                 cluster[ref_index].append(todo[i])
#         else:
#             cluster.append([todo[i]])
#             with mutex:
#                 rep_list.append(todo[i])
#                 rep_cnt[0] += 1
#
#         if i + MAX_QSIZE < len(todo):
#             with mutex:
#                 produceQ.put(i + MAX_QSIZE)
#
#         if i % 1000 == 999:
#             print(f"{i}, qsize: {resultQ.qsize()}, rep_cnt: {rep_cnt[0]}")
#
#     with mutex:
#         produceQ.put(INF)
#
#     for t in threads:
#         t.join()
#
#
# def print_cluster(cluster):
#     for c in cluster:
#         print(len(c), end=" ")
#         print(*c)
#     print()
#
#
# def read_file(name):
#     global N
#     N = 0
#     trace.clear()
#
#     with open(name, 'rb') as f:
#         while True:
#             data = f.read(BLOCK_SIZE)
#             if not data:
#                 break
#             trace.append(data)
#             N += 1
#
#
# def main():
#     global NUM_THREAD
#
#     import sys
#
#     if len(sys.argv) != 3:
#         print("usage: ./coarse [input_file] [num_thread]")
#         sys.exit(0)
#
#     NUM_THREAD = int(sys.argv[2])
#
#     read_file(sys.argv[1])
#
#     dedup = set()
#     unique_list = []
#     for i in range(N):
#         h = hashlib.md5(trace[i]).hexdigest()
#
#         if h in dedup:
#             continue
#         else:
#             dedup.add(h)
#             unique_list.append(i)
#
#     cluster = []
#     bruteForceCluster(unique_list, cluster, COARSE_T)
#
#     unique_list.clear()
#     newcluster = []
#     for i in range(len(cluster)):
#         lz4_min = INF
#         rep = -1
#         for j in cluster[i]:
#             r = random.random()
#             if r < (len(cluster[i]) - 1000) / len(cluster[i]):
#                 continue
#
#             s = sum(do_xdelta3(j, k, 0) for k in cluster[i])
#
#             if s < lz4_min:
#                 lz4_min = s
#                 rep = j
#
#         newcluster.append([rep])
#         for j in cluster[i]:
#             if j != rep:
#                 unique_list.append(j)
#
#     bruteForceCluster(unique_list, newcluster, COARSE_T)
#     print_cluster(newcluster)
#
#
# if __name__ == "__main__":
#     main()
#
