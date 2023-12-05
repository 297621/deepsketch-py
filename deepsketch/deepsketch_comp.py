import os
import sys
from itertools import chain
from collections import deque
import xxhash
import lz4.block
from ngt import Index

INF = 987654321
BLOCK_SIZE = 4096

class DATA_IO:
    def __init__(self, filename):
        self.filename = filename
        self.trace = []
        self.N = 0

    def read_file(self):
        with open(self.filename, 'rb') as f:
            while True:
                data = f.read(BLOCK_SIZE)
                if not data:
                    break
                self.trace.append(data)
                self.N += 1

    def write_file(self, data, size):
        # Implement according to your requirements
        pass

    def recipe_insert(self, recipe):
        # Implement according to your requirements
        pass

    def recipe_write(self):
        # Implement according to your requirements
        pass

    def time_check_start(self):
        # Implement according to your requirements
        pass

    def time_check_end(self):
        # Implement according to your requirements
        pass

class NetworkHash:
    def __init__(self, size, script_module):
        # Implement according to your requirements
        pass

    def push(self, data, index):
        # Implement according to your requirements
        pass

    def request(self):
        # Implement according to your requirements
        pass

class ANN:
    def __init__(self, *args, **kwargs):
        # Implement according to your requirements
        pass

    def request(self, h):
        # Implement according to your requirements
        pass

    def insert(self, h, index):
        # Implement according to your requirements
        pass

class RECIPE:
    # Implement according to your requirements
    pass

def set_offset(recipe, total):
    # Implement according to your requirements
    pass

def set_size(recipe, size):
    # Implement according to your requirements
    pass

def set_ref(recipe, ref):
    # Implement according to your requirements
    pass

def set_flag(recipe, flag):
    # Implement according to your requirements
    pass

def main():
    if len(sys.argv) != 4:
        print("usage: ./ann_inf [input_file] [script_module] [threshold]")
        sys.exit(0)

    threshold = int(sys.argv[3])

    f = DATA_IO(sys.argv[1])
    f.read_file()

    dedup = {}
    dedup_lazy_recipe = deque()
    network = NetworkHash(256, sys.argv[2])

    indexPath = "ngtindex"
    property = Index.Property()
    property.dimension = HASH_SIZE // 8
    property.objectType = Index.ObjectSpace.ObjectType.Uint8
    property.distanceType = Index.Property.DistanceType.DistanceTypeHamming
    Index.create(indexPath, property)
    index = Index(indexPath)

    ann = ANN(20, 128, 16, threshold, property, index)

    total = 0
    f.time_check_start()
    for i in range(f.N):
        h = xxhash.xxh64(f.trace[i]).hexdigest()

        if h in dedup:
            dedup_lazy_recipe.append((i, dedup[h]))
            continue

        dedup[h] = i

        if network.push(f.trace[i], i):
            myhash = network.request()
            for j in range(len(myhash)):
                r = RECIPE()

                h, index = myhash[j]

                comp_self = lz4.block.compress(f.trace[index], store_size=True)
                dcomp_ann = INF
                dcomp_ann_ref = ann.request(h)

                if dcomp_ann_ref != -1:
                    dcomp_ann = xdelta3_compress(f.trace[index], BLOCK_SIZE, f.trace[dcomp_ann_ref], BLOCK_SIZE, delta_compressed, 1)

                set_offset(r, total)

                if min(len(comp_self), BLOCK_SIZE) > dcomp_ann:
                    set_size(r, dcomp_ann - 1)
                    set_ref(r, dcomp_ann_ref)
                    set_flag(r, 0b11)
                    f.write_file(delta_compressed, dcomp_ann)
                    total += dcomp_ann
                else:
                    if len(comp_self) < BLOCK_SIZE:
                        set_size(r, len(comp_self) - 1)
                        set_flag(r, 0b01)
                        f.write_file(compressed, len(comp_self))
                        total += len(comp_self)
                    else:
                        set_flag(r, 0b00)
                        f.write_file(f.trace[index], BLOCK_SIZE)
                        total += BLOCK_SIZE

                ann.insert(h, index)

                while dedup_lazy_recipe and dedup_lazy_recipe[0][0] < index:
                    rr = RECIPE()
                    set_ref(rr, dedup_lazy_recipe.popleft()[1])
                    set_flag(rr, 0b10)
                    f.recipe_insert(rr)

                f.recipe_insert(r)

    myhash = network.request()
    for j in range(len(myhash)):
        r = RECIPE()

        h, index = myhash[j]

        comp_self = lz4.block.compress(f.trace[index], store_size=True)
        dcomp_ann = INF
        dcomp_ann_ref = ann.request(h)

        if dcomp_ann_ref != -1:
            dcomp_ann = xdelta3_compress(f.trace[index], BLOCK_SIZE, f.trace[dcomp_ann_ref], BLOCK_SIZE, delta_compressed, 1)

        set_offset(r, total)

        if min(len(comp_self), BLOCK_SIZE) > dcomp_ann:
            set_size(r, dcomp_ann - 1)
            set_ref(r, dcomp_ann_ref)
            set_flag(r, 0b11)
            f.write_file(delta_compressed, dcomp_ann)
            total += dcomp_ann
        else:
            if len(comp_self) < BLOCK_SIZE:
                set_size(r, len(comp_self) - 1)
                set_flag(r, 0b01)
                f.write_file(compressed, len(comp_self))
                total += len(comp_self)
            else:
                set_flag(r, 0b00)
                f.write_file(f.trace[index], BLOCK_SIZE)
                total += BLOCK_SIZE

        ann.insert(h, index)

        while dedup_lazy_recipe and dedup_lazy_recipe[0][0] < index:
            rr = RECIPE()
            set_ref(rr, dedup_lazy_recipe.popleft()[1])
            set_flag(rr, 0b10)
            f.recipe_insert(rr)

        f.recipe_insert(r)

    f.recipe_write()
    print("Total time:", f.time_check_end(), "us")

    print(f"ANN {sys.argv[1]} with model {sys.argv[2]}")
    print(f"Final size: {total} ({(total * 100) / (f.N * BLOCK_SIZE):.2f}%)")

if __name__ == "__main__":
    main()
