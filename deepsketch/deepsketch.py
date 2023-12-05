import torch
import torch.jit
import torch.cuda
import torch.nn.functional as F
import torch.nn as nn
import xxhash
import NGT
import numpy as np

BLOCK_SIZE = 4096
HASH_SIZE = 128

class NetworkHash:
    def __init__(self, BATCH_SIZE, module_name):
        self.BATCH_SIZE = BATCH_SIZE
        self.module = torch.jit.load(module_name)
        self.module.to(torch.device("cuda"))
        self.module.eval()
        self.data = np.zeros((BATCH_SIZE, BLOCK_SIZE), dtype=np.float32)
        self.memout = np.zeros((BATCH_SIZE, HASH_SIZE), dtype=np.bool)
        self.index = np.zeros(BATCH_SIZE, dtype=np.int32)
        self.cnt = 0

    def __del__(self):
        del self.data
        del self.memout
        del self.index

    def push(self, ptr, label):
        for i in range(BLOCK_SIZE):
            self.data[self.cnt * BLOCK_SIZE + i] = (ord(ptr[i]) - 128) / 128.0

        self.index[self.cnt] = label
        self.cnt += 1

        if self.cnt == self.BATCH_SIZE:
            return True
        else:
            return False

    def request(self):
        if self.cnt == 0:
            return []

        inputs = [torch.from_numpy(self.data[:self.cnt, :]).to(torch.device("cuda"))]
        output = self.module.forward(inputs).to("cpu").to(torch.float32)

        comp = output.ge(0.0)
        self.memout[:self.cnt, :] = comp.cpu().numpy()

        ret = []
        for i in range(self.cnt):
            hash_value = np.zeros(HASH_SIZE, dtype=np.bool)
            for j in range(HASH_SIZE):
                if self.memout[i, j]:
                    hash_value[j] = True

            ret.append((hash_value, self.index[i]))

        self.cnt = 0
        return ret

class ANN:
    def __init__(self, ANN_SEARCH_CNT, LINEAR_SIZE, NUM_THREAD, THRESHOLD, property, index):
        self.ANN_SEARCH_CNT = ANN_SEARCH_CNT
        self.LINEAR_SIZE = LINEAR_SIZE
        self.NUM_THREAD = NUM_THREAD
        self.THRESHOLD = THRESHOLD
        self.property = property
        self.index = index
        self.linear = []
        self.hashtable = {}

    def request(self, h):
        dist = 999
        ret = -1

        for i in range(len(self.linear) - 1, -1, -1):
            nowdist = np.count_nonzero(self.linear[i] ^ h)
            if dist > nowdist:
                dist = nowdist
                ret = self.hashtable[self.linear[i]][-1]

        query = []
        for i in range(self.property.dimension):
            query.append((h << (HASH_SIZE - 8 * i - 8)) >> (HASH_SIZE - 8) & 0xFF)

        sc = NGT.SearchQuery(query)
        objects = NGT.ObjectDistances()
        sc.setResults(objects)
        sc.setSize(self.ANN_SEARCH_CNT)
        sc.setEpsilon(0.2)
        self.index.search(sc)

        for i in range(objects.size()):
            nowdist = objects[i].distance

            if dist > nowdist:
                now = np.zeros(HASH_SIZE, dtype=np.bool)

                object_space = self.index.getObjectSpace()
                obj = object_space.getObject(objects[i].id)

                for j in range(object_space.getDimension()):
                    for k in range(8):
                        if obj[j] & (1 << k):
                            now[8 * j + k] = True

                dist = nowdist
                ret = self.hashtable[tuple(now)][-1]
            elif dist == nowdist:
                now = np.zeros(HASH_SIZE, dtype=np.bool)

                object_space = self.index.getObjectSpace()
                obj = object_space.getObject(objects[i].id)

                for j in range(object_space.getDimension()):
                    for k in range(8):
                        if obj[j] & (1 << k):
                            now[8 * j + k] = True

                nowindex = self.hashtable[tuple(now)][-1]

                if nowindex > ret:
                    ret = nowindex

        if dist <= self.THRESHOLD:
            return ret
        else:
            return -1

    def insert(self, h, label):
        if tuple(h) in self.hashtable:
            self.hashtable[tuple(h)].append(label)
            return

        self.hashtable[tuple(h)] = [label]
        self.linear.append(h)

        if len(self.linear) == self.LINEAR_SIZE:
            for i in range(len(self.linear)):
                query = []
                for j in range(self.property.dimension):
                    query.append((self.linear[i] << (HASH_SIZE - 8 * j - 8)) >> (HASH_SIZE - 8) & 0xFF)
                self.index.append(query)

            self.index.createIndex(self.NUM_THREAD)
            self.linear.clear()

# Example Usage
BATCH_SIZE = 32
ANN_SEARCH_CNT = 10
LINEAR_SIZE = 100
NUM_THREAD = 4
THRESHOLD = 5

property = NGT.Property()
property.dimension = HASH_SIZE // 8
property.objectType = NGT.ObjectSpace.ObjectType.Uint8
property.distanceType = NGT.Index.Property.DistanceType.DistanceTypeHamming

index_path = "ngtindex"
NGT.Index.create(index_path, property)
index = NGT.Index(index_path)

network = NetworkHash(BATCH_SIZE, "your_module_name.pth")
ann = ANN(ANN_SEARCH_CNT, LINEAR_SIZE, NUM_THREAD, THRESHOLD, property, index)

# Use the network and ANN as needed...
