import threading
import hashlib

BLOCK_SIZE = 4096
MAX_THREAD = 256
INF = 987654321

trace = []
unique_block = []
N = 0

file_name_temp = ""
file_name_result = ""
fp_temp = None

result = []
todo = set()
mutex = threading.Lock()


def read_file(name):
    global N
    with open(name, 'rb') as f:
        while True:
            data = f.read(BLOCK_SIZE)
            if not data:
                break
            trace.append(data)
            N += 1


def restore_result(name):
    try:
        with open(file_name_temp, 'rt') as f:
            for line in f:
                num, ref, size = map(int, line.strip().split())
                result.append((num, ref, size))
                todo.discard(num)
    except FileNotFoundError:
        pass


def print_result(name):
    total = sum(u[2] for u in result)
    result.sort()

    with open(file_name_result, 'wt') as f:
        f.write(f"{total} {total / N / BLOCK_SIZE * 100:.2f}\n")
        for u in result:
            f.write(f"{u[0]} {u[1]} {u[2]}\n")


def func(arg):
    while True:
        with mutex:
            if not todo:
                break
            i = todo.pop()

        size = len(trace[i])
        ref = -1

        for j in range(i):
            if not unique_block[j]:
                continue
            now = len(xdelta3_compress(trace[i], trace[j]))
            if now < size:
                size = now
                ref = j

        with mutex:
            result.append((i, ref, size))
            fp_temp.write(f"{i} {ref} {size}\n")
            if i % 100 == 0:
                print(f"{i}/{N}", end="\r")


def main():
    global file_name_temp, file_name_result, fp_temp

    import sys

    if len(sys.argv) != 3:
        print("usage: ./bf [file_name] [num_thread]")
        sys.exit(0)

    file_name_temp = f"{sys.argv[1]}_bf_temp"
    file_name_result = f"{sys.argv[1]}_bf_result"

    NUM_THREAD = int(sys.argv[2])

    read_file(sys.argv[1])
    unique_block = [False] * N

    dedup = set()
    for i in range(N):
        h = hashlib.md5(trace[i]).hexdigest()
        if h not in dedup:
            todo.add(i)
            dedup.add(h)
            unique_block[i] = True

    restore_result(sys.argv[1])

    threads = []
    fp_temp = open(file_name_temp, "at")

    for i in range(NUM_THREAD):
        t = threading.Thread(target=func, args=(i,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    fp_temp.close()
    print_result(sys.argv[1])


if __name__ == "__main__":
    main()
