import lz4.block
import queue
import threading
from queue import PriorityQueue

BLOCK_SIZE = 4096
INF = 987654321
MAX_THREAD = 256

N = 0
NUM_THREAD = 0
trace = []
mutex = threading.Lock()
coarse_result="D:/学习资料/deepsketch/deepsketch-py/clustering/result/mix_coarse"


def do_lz4(i, j):
    return lz4.block.compress(trace[i])


class RepArgument:
    def __init__(self, _id, _cluster, _todoQ, _doneQ):
        self.id = _id
        self.cluster = _cluster
        self.todoQ = _todoQ
        self.doneQ = _doneQ


def RP(arg):
    while True:
        i, j = -1, -1
        with mutex:
            if not arg.todoQ.empty():
                i, j = arg.todoQ.get()
            else:
                break

        if i != -1:
            total = sum(len(do_lz4(i, k)[0]) for k in arg.cluster[i])
            with mutex:
                arg.doneQ.put((total, j))


def choose_rep(cluster, todo):
    todo.clear()
    new_rep = []

    max_elem = max(len(subcluster) for subcluster in cluster)

    if max_elem < NUM_THREAD:
        for i in range(len(cluster)):
            sum_min = INF
            ref = -1
            for u in cluster[i]:
                total = sum(do_lz4(u, v)[0] for v in cluster[i] if u != v)
                if total < sum_min:
                    sum_min = total
                    ref = u
            for u in cluster[i]:
                if u == ref:
                    new_rep.append(u)
                else:
                    todo.append(u)
        cluster.clear()
        for u in new_rep:
            cluster.append([u])
        return
    print('%')
    mutex = threading.Lock()
    todoQ = queue.Queue()
    doneQ = queue.Queue()

    arg = RepArgument(NUM_THREAD - 1, cluster, todoQ, doneQ)
    threads = [threading.Thread(target=RP, args=(arg,)) for _ in range(NUM_THREAD - 1)]

    for t in threads:
        t.start()
    print('^')
    for i in range(len(cluster)):
        if len(cluster[i]) == 1:
            new_rep.append(cluster[i][0])
            continue

        lz4_min = INF
        rep = -1
        for j in range(len(cluster[i])):
            with mutex:
                todoQ.put((i, j))
        cnt = 0
        print('&')
        while cnt < len(cluster[i]):
            with mutex:
                if not doneQ.empty():
                    lz4, idx = doneQ.get()
                    if lz4 < lz4_min:
                        lz4_min = lz4
                        rep = idx
                    cnt += 1
                if doneQ.empty():
                    break
        for j in range(len(cluster[i])):
            if j != rep:
                todo.append(cluster[i][j])
            else:
                new_rep.append(cluster[i][j])
    print('*')
    with mutex:
        todoQ.put((INF, -1))

    for t in threads:
        t.join()

    cluster.clear()
    for u in new_rep:
        cluster.append([u])


def brute_force_cluster(todo, cluster, threshold):
    if len(todo) < NUM_THREAD:
        for u in todo:
            compress_min = INF
            ref_index = -1
            for i in range(len(cluster)):
                lz4 = len(do_lz4(u, cluster[i][0]))
                if lz4 < compress_min:
                    compress_min = lz4
                    ref_index = i
            if compress_min <= threshold:
                cluster[ref_index].append(u)
            else:
                cluster.append([u])
        return

    MAX_QSIZE = 2 * NUM_THREAD
    mutex = threading.Lock()

    rep_list = cluster[::]
    rep_cnt = len(rep_list)

    # Priority Queue: {index in todo, checked index of rep_list, size, ref index of rep_list}
    produceQ = queue.Queue()
    resultQ = PriorityQueue()

    for i in range(min(MAX_QSIZE, len(todo))):
        produceQ.put(i)

    class BruteForceClusterArgument:
        pass

    arg = BruteForceClusterArgument()
    arg.mutex = mutex
    arg.id = NUM_THREAD - 1
    arg.todo = todo
    arg.rep_list = rep_list
    arg.rep_cnt = rep_cnt
    arg.produceQ = produceQ
    arg.resultQ = resultQ

    # Create thread
    threads = [threading.Thread(target=BF, args=(arg,))]

    for t in threads:
        t.start()

    for i in range(len(todo)):
        print(i,len(todo))
        print(resultQ.qsize)
        while True:
            with mutex:
                if not resultQ.empty():
                    now = resultQ.get()
                if now[0] == i:
                    break
                if resultQ.empty():
                    break

        print('!')
        for k in range(5):
            if len(now)<k:
                now=now+(-1,)
        #print(len(now))
        compress_min = now[2]
        ref_index = now[3]
        for j in range(now[1], rep_cnt):
            lz4 = len(do_lz4(todo[i], rep_list[j])[0])
            if lz4 < compress_min:
                compress_min = lz4
                ref_index = j
        print("@")
        if len(cluster)==0:
            cluster.append([])
        if compress_min <= threshold:
            cluster[ref_index].append(todo[i])
        else:
            cluster.append([todo[i]])
            with mutex:
                rep_list.append(todo[i])
        print('#')
        if i + MAX_QSIZE < len(todo):
            with mutex:
                produceQ.put(i + MAX_QSIZE)

    with mutex:
        produceQ.put(INF)

    for t in threads:
        t.join()


def read_file(name):
    global N, trace
    N = 0
    trace = []

    with open(name, "rb") as f:
        while True:
            ptr = bytearray(BLOCK_SIZE)
            now = f.readinto(ptr)
            #print(ptr)
            if not now:
                break
            trace.append(ptr)
            N += 1


def BF(arg):
    while True:
        i = -1
        with arg.mutex:
            if not arg.produceQ.empty():
                i = arg.produceQ.get()
            else:
                break

        if i != -1:
            compress_min = INF
            ref_index = -1
            for j in range(arg.rep_cnt):
                lz4 = len(do_lz4(arg.todo[i], arg.rep_list[j])[0])
                if lz4 < compress_min:
                    compress_min = lz4
                    ref_index = j

            with arg.mutex:
                arg.resultQ.put((i, compress_min, ref_index))


def read_input_file(file_path):
    data = []
    with open(file_path, 'r', encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            parts = [int(x) for x in line.strip().split()]
            data.append(parts)
    return data

def main(input_file, num_thread):
    global N, NUM_THREAD, trace
    NUM_THREAD = num_thread
    read_file(input_file)

    lines = read_input_file(coarse_result)

    i = 0
    while i < len(lines):
        #print(lines[i])
        data_line = lines[i]
        if len(data_line) == 0:
            break
        sz = int(data_line[0])
        unique_list = list(map(int, data_line[1:]))

        if sz == 1:
            print(f"{sz} {unique_list[0]}")
            i += 2
            continue

        print(f"Cluster of size {sz}: {unique_list[0]}")

        ans = []
        optimal = INF
        op_th = 0

        for threshold in range(64, 513, 64):
            cluster = []
            rep_pre = set()
            brute_force_cluster(unique_list, cluster, threshold)
            for epoch in range(2):
                todo = []
                choose_rep(cluster, todo)
                #print(1)
                brute_force_cluster(todo, cluster, threshold)

            lz4_sum = sum(len(do_lz4(cluster[i][0], cluster[i][j])) for i in range(len(cluster)) for j in range(1, len(cluster[i])))

            print(f"Threshold {threshold}: Total cnt {len(todo)}, Total lz4 {lz4_sum}")

            if lz4_sum < optimal:
                optimal = lz4_sum
                op_th = threshold
                ans = cluster
            elif lz4_sum > optimal:
                break

        print(f"Best: {optimal} (Threshold: {op_th})")

        for i in range(len(ans)):
            print(f"{len(ans[i])} {' '.join(map(str, ans[i]))}")

        i += 2

if __name__ == "__main__":
    import sys

    if len(sys.argv) != 3:
        print("usage: ./fine [input_file] [num_thread]")
        sys.exit(0)

    main(sys.argv[1], int(sys.argv[2]))
