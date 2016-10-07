from heapq import *
import itertools


class PriorityQueue:
    def __init__ (self):
        self.REMOVED = 'removed-url'
        self.counter = itertools.count()
        self.urlDict = {}
        self.pq = []

    def add(self, url, priority, depth):
        prevEntry = None
        prevPriority = 0.0
        prevNrLinks = 0
        if url in self.urlDict:
            prevEntry = self.removeUrl(url)
        # count = next(self.counter)
        if prevEntry is not None:
            prevPriority = prevEntry[0]* (-1)
            prevNrLinks = prevEntry[1]
            priority = ((prevNrLinks * prevPriority) + priority)/ (prevNrLinks + 1)
        entry = [(-1)*priority, prevNrLinks + 1, depth, url]
        self.urlDict[url] = entry
        heappush(self.pq, entry)

    def removeUrl(self, url):
        entry = self.urlDict.pop(url)
        entry[-1] = self.REMOVED
        return entry

    def printPQ(self):
        a = nsmallest(2, self.pq)
        print ("Larges are")
        print (a)

    def popUrl(self):
        while self.pq:
            priority, count, depth, url = heappop(self.pq)
            if url is not self.REMOVED:
                del self.urlDict[url]
                return {'url' : url, 'depth' : depth, 'priority' : priority}
        raise KeyError('pop from an empty priority queue')

    def length(self):
        return len(self.urlDict)


if __name__ == "__main__":
   pq = PriorityQueue()
   pq.add('abc.com', 2, 1)
   pq.add('abcd.com', 8, 2)
   pq.add('abcd.com', 6, 3)
   pq.add('abcd.com', 6, 4)
   pq.add('abcd.com', 6, 5)
   pq.printPQ()
   a = pq.popUrl()
   print(a.get('url'))
   print(a.get('count'))
   pq.add('abcde.com', 6, 4)
   a = pq.popUrl()
   print(a.get('url'))
   print(a.get('count'))
   pq.add('abcde.com', 4, 2)
   pq.add('abcdef.com', 6, 7)
   pq.add('abcdef.com', 8, 8)
   a = pq.popUrl()
   print(a.get('url'))
   print (a.get('depth'))
   print (a.get('count'))
   print (pq.length())
