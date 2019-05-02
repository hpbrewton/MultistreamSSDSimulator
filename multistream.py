import heapq as hq
import random
from itertools import tee
import json

class Block:
    def __init__(self, size):
        self.nfree = size 
        self.ninvalid = 0 
        self.nvalid = 0
    
    def __str__(self):
        return str((self.nfree, self.ninvalid, self.nvalid))

class FlashBank:
    def __init__(self, nblocks, pageperblock, pagesize):
        self.nblocks = pageperblock
        self.pageperblock = pageperblock
        self.blockmap = {}

    def erase(self, block):
        if block not in self.blockmap:
            return False
        else:
            del self.blockmap[block]
            return True

    def program(self, block):
        # page is not used but passed for consistency of arch
        if block not in self.blockmap:
            self.blockmap[block] = Block(self.pageperblock)
        if self.blockmap[block].nfree > 0: 
            self.blockmap[block].nfree -= 1
            self.blockmap[block].nvalid += 1
            return True 
        else:
            return False
        
    def read(self, block):
        return None

    def hasErasedPage(self, block):
        if block in self.blockmap:
            return self.blockmap[block].nfree > 0
        else:
            self.blockmap[block] = Block(self.pageperblock)
            return True

    def nvalid(self, block):
        if block not in self.blockmap:
            return float("inf") 
        else: 
            return self.blockmap[block].nvalid/self.pageperblock

    def numvalid(self, block):
        if block not in self.blockmap:
            return 0
        else: 
            return self.blockmap[block].nvalid 

    def trim(self, block):
        if block not in self.blockmap:
            return False 
        self.blockmap[block].nvalid -= 1
        self.blockmap[block].ninvalid += 1
        return True

    def show(self):
        for block in range(self.nblocks):
            if block in self.blockmap:
                yield ("Block {}: {}".format(block, self.blockmap[block]))
                    
class FTL:
    def __init__(self, nbanks, nblocks, npages, pagesize): 
        self.nbanks = nbanks
        self.nblocks = nblocks 
        self.npages = npages
        self.pagesize = pagesize

        self.banks = {} #[FlashBank(nblocks, npages, pagesize) for _ in range(self.nbanks)]

        self.blocksize = npages*self.pagesize
        self.banksize = nblocks*self.blocksize

    def pageModuloDevice(self, page): 
        page = lpage%self.npages
        lblock = lpage//self.npages
        block = lblock%self.nblocks
        bank = lblock//self.nblocks
        return (bank, block, page)

    def show(self):
        for bank in range(self.nbanks):
            if bank in self.banks:
                yield ("bank {}:".format(bank))
                for line in self.banks[bank].show():
                    yield ("    {}".format(line))

# Log Stuctured FTL
class LSFTL(FTL):
    def __init__(self, nbanks, nblocks, npages, pagesize): 
        super().__init__(nbanks, nblocks, npages, pagesize)
        self.dToDie = 3
        self.table = {}
        def mediaIter():
            while True:
                for bank in range(nbanks):
                    for block in range(nblocks):
                        for page in range(npages):
                            yield (bank, block)
        self.mediaIter = mediaIter()
        self.streamMap = {}
        self.cache = []
        self.cacheSize = 488
        self.usage = 0
        self.cacheOn = False

    def translate(self, lpage):
        return self.table[lpage]

    def trim(self, lpage):
        if lpage not in self.table:
            return False
        (bank, block) = self.translate(lpage)
        del self.table[lpage]
        return self.banks[bank].trim(block)

    # uses LRU
    def inCache(self, lpage):
        if lpage in self.cache:
            self.cache.remove(lpage)
            self.cache.append(lpage)
            return True
        else:
            self.cache = self.cache[1:]
            self.cache.append(lpage)
            return False

    def write(self, lpage, stream = 0):
        # if self.inCache(lpage):
        #     return True

        # kill old write 
        if lpage in self.table:
            assert self.trim(lpage)
        
        # do new write
        if self.usage >= (self.nbanks*self.nblocks*self.npages):
            return False
        self.usage += 1
        if stream in self.streamMap:
            bank = self.streamMap[stream][0]
            block = self.streamMap[stream][1]
            if self.banks[bank].hasErasedPage(block):
                assert self.banks[bank].program(block)
                self.table[lpage] = (bank, block)
                return True
            else:
                del self.streamMap[stream]
        currIndex = self.mediaIter.__next__()
        while ((currIndex[0] in self.banks) and not self.banks[currIndex[0]].hasErasedPage(currIndex[1])) or (currIndex[:2] in self.streamMap.values()):
            currIndex = self.mediaIter.__next__()
        (bank, block) = currIndex
        if bank not in self.banks:
            self.banks[bank] = FlashBank(self.nblocks, self.npages, self.pagesize)
        self.streamMap[stream] = currIndex[:2]
        assert self.banks[bank].program(block)
        self.table[lpage] = currIndex
        return True 

    def unsafeFree(self, bank, block):
        if (bank , block) in self.streamMap.values():
            keys = list(self.streamMap.keys())
            for key in keys:
                if self.streamMap[key] == (bank, block):
                    del self.streamMap[key]            
        self.banks[bank].erase(block)

    def read(self):
        pass 

    def garbageCollect(self):
        def blockIter():
            for bank in range(self.nbanks):
                for block in range(self.nblocks):
                    yield (bank,block)
        blocks = {block for block in hq.nsmallest(self.dToDie, blockIter(), lambda i: self.banks[i[0]].nvalid(i[1]))}
        npages = sum(map(lambda block: self.banks[block[0]].numvalid(block[1]), blocks))

        for block in blocks:
            self.unsafeFree(*block)
        invalidKeys = (list(filter(lambda lpage: (self.translate(lpage)[:2] in blocks), self.table.keys())))
        for key in invalidKeys:
            del self.table[key]
        self.streamMap = {}
        for page in invalidKeys:
            self.write(page)

    def showZCurve(self):
        zcurveData = {}
        for bank in range(ftl.nbanks):
            if bank not in ftl.banks:
                continue
            for block in range(ftl.nblocks):
                if (ftl.banks[bank].nvalid(block)) != float("inf"):
                    if ftl.banks[bank].nvalid(block) not in zcurveData:
                        zcurveData[ftl.banks[bank].nvalid(block)] = 0
                    zcurveData[ftl.banks[bank].nvalid(block)] += 1
        if 0 in zcurveData:
            del zcurveData[0]
        print(json.dumps(zcurveData))

    def runLegacy(self, workload):
        for item in workload:
            if item[0] == "OP_WRITE":
                self.write(item[1], stream=0)
            if item[0] == "OP_TRIM":
                self.trim(item[1])
        self.showZCurve()

    def runMulti(self, workload):
        for item in workload:
            if item[0] == "OP_WRITE":
                self.write(item[1], stream=item[2])
            if item[0] == "OP_TRIM":
                self.trim(item[1])
        self.showZCurve()

# TODO simulate a single purpose computer

def everyOther():
    for i in range(1000000):
        yield ("OP_WRITE", i, 1+(i%2))
    for j in range(1000000):
        if j%2 == 0:
            yield ("OP_TRIM", j)

def metaData():
    for i in range(1000000):
        if random.random() > 0.8:
            yield ("OP_WRITE", i%10, 1)
        yield ("OP_WRITE", i, 2)

def hotAndCold():
    for i in range(1000000):
        if random.random() > 0.8:
            yield ("OP_WRITE", random.randint(0, 10000), 1)
        else:
            yield ("OP_WRITE", random.randint(10000, 20000), 2)

def lagAndLead():
    lag = 0
    lead = 0
    while (lag < 1000000 or lead < 1000000):
        assert lag <= lead
        if lead >= 1000000:
            yield ("OP_TRIM", lag)
            lag += 1 
            continue
        if lag == lead:
            for i in range(1000):
                yield ("OP_WRITE", lead, 1)
                lead += 1000
        else:
            if random.random() > 0.5:
                yield ("OP_TRIM", lag)
                lag +=1 
            else:
                yield ("OP_WRITE", lead, 1)
                lead +=1 

def pathological():
    for i in range(10000):
        yield ("OP_WRITE", 0, random.randint(1, 10))


def backgroundNoise(purp, rate = 0.05):
    for item in purp:
        yield item 
        if random.random() < rate: 
            yield ("OP_WRITE", random.randint(1000000, 1001000), 0)

ftl = LSFTL(10, 2048, 128, 4096)
ftl.runLegacy(backgroundNoise(pathological()))
ftl.runMulti(backgroundNoise(pathological()))

