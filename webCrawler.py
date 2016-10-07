import queue
import re
import sys
import urllib
import urllib.request
from datetime import datetime
from http.cookiejar import CookieJar
from time import strftime
from urllib.parse import urlparse
# import robotexclusionrulesparser
import urllib.robotparser
from bs4 import BeautifulSoup
from google import search
from scipy import spatial
import socket
import time

from PriorityQueue import *


class webCrawler:
    def __init__(self, searchStr, n, focused, debug):
        # Initialize all variables
        self.searchStrWrds = searchStr.split()
        self.n = n
        self.debug = debug

        if focused == "True":
            self.focused = True
        else:
            self.focused = False

        outputLocDir = "F:\MSCSNyuPoly\GitHub\WebCrawler\Output"
        if self.focused:
            self.outputFileLoc = outputLocDir + "\CrawledUrlsFocused" + strftime("%m%d%Y%H%M%S") + ".log"
            # self.visitedPagesLoc = outputLocDir + "\VisitedUrlsFocused" + strftime("%m%d%Y%H%M%S") + ".log"
        else:
            self.outputFileLoc = outputLocDir + "\CrawledUrlsBFS" + strftime("%m%d%Y%H%M%S") + ".log"
            # self.visitedPagesLoc = outputLocDir + "\VisitedUrlsBFS" + strftime("%m%d%Y%H%M%S") + ".log"
        # self.modifiedURLLoc = outputLocDir + "\ModifiedUrl" + strftime("%m%d%Y%H%M%S") + ".log"

        self.searchStrWrdsMap = {}
        for word in searchStr.split():
            word = word.lower()
            self.searchStrWrdsMap[word] = self.searchStrWrdsMap.get(word, 0) + 1

        self.timeout = 10
        socket.setdefaulttimeout(self.timeout)

        self.pagesCrawled = 0
        self.relevantCount = 0
        self.outputFile = open(self.outputFileLoc, 'w')
        # self.visitedPagesFile = open(self.visitedPagesLoc, 'w')
        # self.modifiedURLFile = open(self.modifiedURLLoc, 'w')
        self.outputFile.write("Query is %s\n" % searchStr)
        self.outputFile.write("Number of pages to be crawled %s\n" % self.n )
        if self.focused:
            self.outputFile.write("Crawling Type Focused\n\n")
        else:
            self.outputFile.write("Crawling Type BFS\n\n")
        self.outputFile.write("url, currTime, priority, depth, IsPageRelevant, harvestRate, urlSize, returnCode\n")

        self.visitedURL = {}
        self.robotHistory = {}
        self.valid_types = ['text/html', 'text/html; charset=utf-8']
        self.ignoreEnd = ['index.htm', 'index.html', 'index.jsp', 'main.html' ]
        self.validEnd = ['html', 'htm', 'asp', 'aspx', 'php', 'jsp', 'jspx']
        self.invalidPattern = ['#', 'cgi', 'javascript']
        # self.invalidExt = ['js', 'jpeg', 'jpg', 'png', 'gif', 'xml', 'css', 'mp3', 'mp4', 'zip', 'gzip', 'gz', 'rar', 'doc', 'docx', 'xls', 'xlsx', 'pdf' ]
        # self.avoidDomains = ['google.com', 'youtube.com', 'instagram.com', 'facebook.com']
        self.perDomainLimit = 40
        self.perDomainCount = {}
        self.pq = PriorityQueue()
        self.q = queue.Queue()
        self.p = re.compile('(htt(p|ps))://([^/]*).*')
        self.splCharsRegex = re.compile('^[\W_]+$')
        self.count403 = 0
        self.count404 = 0
        self.ttlSize = 0

        self.getGoogleUrl(searchStr)
        # self.checkRobotExclusion("https://eml.wikipedia.org")
        # self.checkRobotExclusion("https://eml.wikipedia.org")
    ###########################################################
    # Find top 10 urls returned by google search for given query. It will avoid taking multiple urls from same hostname
    ###########################################################
    def getGoogleUrl(self, searchStr):
        if (searchStr is None) or (searchStr == ''):
            print("Please provide search query")
            sys.exit(1)
        if self.debug == 1:
            print("Fetching top 10 search query results")
        oldDomain = ""
        gurlCnt = 0
        for url in search(searchStr):
            # print ("Url is %s" % url)
            m = self.p.match(url)
            # To avoid getting multiple results from same domain
            if (m.group(3) != oldDomain):
                # Fetch only those google links where at least one query word is present
                if  (self.checkRobotExclusion(url) == True) and (self.readLink(url, 0, -1) is not None):
                    if self.debug == 1:
                        print("Url %s added to seed pages" % url)
                    gurlCnt += 1
            oldDomain = m.group(3)
            # Get only top 10 results
            if gurlCnt == 10:
                break;
        self.geturlData()


    ###########################################################
    #Fetched and crawl url from priority queue or normal queue until number of crawled pages reached limit or priority queue/queue is empty
    ###########################################################
    def geturlData(self):
        qLen = self.getQueueLength()
        while qLen > 0 and self.pagesCrawled < self.n:
            if self.debug == 1:
                print("Priority queue length %d Pages Crawled %d Max Pages %d" % (qLen, self.pagesCrawled, self.n))
            if self.focused:
                dict = self.pq.popUrl()
            else:
                dict = self.q._get()
            qLen = self.getQueueLength()
            url = dict.get('url')
            depth = dict.get('depth')
            priority = dict.get('priority')
            hostName =self.getUrlComponents(url).get('netloc', None)
            # if self.debug == 1:
            #     print ("Popped url is %s" % url)
            # if self.getDomain(hostName) in self.avoidDomains:
            #     continue
            # Avoid crawling all the links from same hostname
            if self.perDomainCount.get(hostName, 0) >= self.perDomainLimit:
                continue
            self.readLink(url, depth, priority)

    ###########################################################
    # Normalize link
    ###########################################################
    def validateUrl(self, ln, pln, protocol, domain):
        if (ln is None or ln == ''):
            return None
        ln = ln.lower()
        if any( pat in ln for pat in self.invalidPattern):
            return None
        # if ("#" in ln) or ("cgi" in ln) or ("javascript" in ln):
        #     return None
        lnSplit = ln.split("/")
        lnDotSplit = (lnSplit[-1]).split(".")
        #Normalize link only if it ends with / or if its extension is within validEnd
        # or if there is not . in last part of url
        if (ln[-1] == "/") or (lnDotSplit[-1] in self.validEnd) or (not "." in lnSplit[-1]):
            if lnSplit[-1] in self.ignoreEnd:
                ln = "/".join(lnSplit[0:-1])
            if ln == '':
                return None
            # lnSplit = ln.split(".")
            # if lnSplit[-1] in self.invalidExt:
            #     return None
            #If starting with // then add parent links protocol either http or https
            if (ln[:2] == "//"):
                ln = protocol + ":" + ln
            #     If link starting with / then prefix parent links protocol :// and hostname a
            elif (ln[:1] == "/"):
                ln = protocol + "://" + domain + ln
            #     if starting with ../ then go one level up in parent link and prefix that part to link
            elif (ln[:3] == "../"):
                plnSplit = pln.split("/")
                lnSplit = ln.split("/")
                ln = "/".join(plnSplit[0:-2]) + "/" + "/".join(lnSplit[1:])
            # If link not starting with http then just append parent link before link
            elif not (ln[:4] == "http"):
                ln = pln + "/" + ln
            else:
                ln = ln
            #Remove trailing slash from all the links to normalize it
            return self.removeTrailingSlash(ln)
        return None

    ###########################################################
    # Read data from link and get wordPriority based on document contnet
    # If word Priority not 0 then get all hyperlinks from the page and assign them priority
    # based on this pages' word priority and url and anchor text of hyperlinks
    ###########################################################
    def readLink(self, url, depth, priority):
        linksList = {}
        wordPriority = 0
        pageRelevancy = None
        if url in self.visitedURL:
            return
        self.visitedURL[url] = 1
        urlSize = 0
        parentPriority = priority
        returnCode = None
        data = None
        try:
            req = urllib.request.Request(url, None, {'Accept': 'text/html', 'Accept-Language': 'en-US,en;q=0.8'})
            cj = CookieJar()
            opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
            response = opener.open(req)
            data = response.read().decode('utf8', errors='ignore')
            urlSize = len(data)
            urlType = response.info()['Content-Type']
            returnCode = response.getcode()
            response.close()
        except Exception as e:  # catch the error
            try:
                returnCode = e.code
            except Exception:
                returnCode = e
            print("Error in downloading link  Error is %s" % (returnCode))
        if returnCode == 403:
            self.count403 += 1
        elif returnCode == 404:
            self.count404 += 1
        self.ttlSize += urlSize
        if data is not None:
            try:
                soup = BeautifulSoup(data, 'html.parser')
            except Exception:
                return pageRelevancy
            wordPriority = self.getPriority(soup, url)
            print ("Word Priority is %f" % wordPriority)
            #If word priority o then there is no meaning in storing hyperlinks from that page
            if not wordPriority == 0.0:
                self.getLinks(soup, url, depth, wordPriority)
                # print("Got Links")

            self.addPerDomainCount(url)
            #If there is at least one query word present in content that is priority not 0 then
            #consider page as Relevant
            if not wordPriority == 0:
                pageRelevancy = 'Relevant'
                self.relevantCount += 1
            else:
                pageRelevancy = 'NonRelevant'
            self.pagesCrawled += 1
            harvestRate = self.relevantCount / self.pagesCrawled
            self.outputFile.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % (url, strftime("%m%d%Y%H%M%S"), parentPriority*(-1), depth, pageRelevancy, harvestRate, urlSize, returnCode))

        return pageRelevancy

    #get cosine Similarity for content of link
    def getPriority(self, soup, url):
        words = soup.get_text().split()
        # print ("Getting priority for size %d" % len(words))
        return self.docCosineSimilarity(words, True)
#####################################################################################

    # get hyperlink from content and then add those links in priority queue/queue of valid
    # along with modified priority which includes url and anchor text priority
    def getLinks(self, soup, url, depth, wordPriority):
        linksList = {}
        linkCnt = 0
        m = self.p.match(url)
        protocol = m.group(1)
        domain = m.group(3)

        for link in soup.find_all('a'):
            # if self.debug == 1:
            #     print("Original link is %s" % link.get('href'))
            ln = self.validateUrl(link.get('href'), url, protocol, domain)
            if ln is not None:
                linksList[ln] = link.get('title')
        # print("Normalized all urls")
        for ln in linksList:
            if ln not in self.visitedURL and self.checkRobotExclusion(ln) == True:
                title = link.get('title')
                titlePriority = 0
                if title is not None:
                    titlePriority = self.docCosineSimilarity(title.split(), False)
                #Make array of query words which are present in link to find cosine similarity
                linkWordArr = []
                for word in self.searchStrWrdsMap:
                    if word in ln:
                        linkWordArr.append(word)
                urlPriority = self.docCosineSimilarity(linkWordArr, False)
                priority = wordPriority + titlePriority + urlPriority
                # print ("Word priority is %f Title Priority is %f UrlPrioroty is %f" % (wordPriority, titlePriority, urlPriority))
                ################################################################################################################
                if self.focused:
                    self.pq.add(ln, priority, depth + 1)
                else:
                    self.q._put({'url': ln, 'priority': priority, 'depth': depth + 1})
                linkCnt += 1


   # Check if it is allowed to crawl given link
    def checkRobotExclusion(self, url):
        # rerp = robotexclusionrulesparser.RobotExclusionRulesParser()
        # try:
        #     o = self.getUrlComponents(url)
        #     robotFilePath = o.get('scheme') + "://" + o.get('netloc') + "/robots.txt"
        #     rerp.fetch(robotFilePath, timeout=10)
        #     return rerp.is_allowed("*", url)
        # except Exception as e:
        #     print("Error in verifying robot exclusion Error is %s Robot Path is %s" % (e, robotFilePath))
        # return False
        o = self.getUrlComponents(url)
        robotFilePath = (o.get('scheme') + "://" + o.get('netloc') + "/robots.txt")
        try:
            if (url in self.robotHistory):
                return self.robotHistory.get(url, False)
            rp = urllib.robotparser.RobotFileParser()
            rp.set_url(robotFilePath)
            rp.read()
            isAllowed = rp.can_fetch("*", url)

        except Exception as e:
            isAllowed = False
            print("Error in verifying robot exclusion Error is %s Robot Path is %s" % (e, robotFilePath))
        self.robotHistory[url] = isAllowed
        return isAllowed

    def getUrlComponents(self, url):
        o = urlparse(url)
        return {'scheme' : o.scheme, 'netloc' : o.netloc, 'path' : o.path}

    def removeTrailingSlash(self, url):
        if url.endswith("/"):
            return url[:-1]
        return url

    def addPerDomainCount(self, url):
        o = self.getUrlComponents(url)
        if (o.get('netloc') in self.perDomainCount):
            self.perDomainCount[o.get('netloc')] = self.perDomainCount.get(o.get('netloc')) + 1
        else:
            self.perDomainCount[o.get('netloc')] = 1

    def getQueueLength(self):
        if self.focused:
            return self.pq.length()
        else:
            return self.q._qsize()

    def getDomain(self, host):
        return host.partition('.')[2]


    def docCosineSimilarity(self, words, isText):
        # If there is no text then return 0
        if len(words) <= 0:
            return 0
        docDict = {}
        ttlDocWords = 0
        isWordP = False
        # Make a dictionary of word and frequency for words which are present in query
        for word in words:
            word = word.lower()
            #If word contains only special characters then ingnore that word from total count
            if (self.splCharsRegex.match(word) is not None):
                continue
            ttlDocWords += 1
            if word in self.searchStrWrdsMap:
                isWordP = True
                docDict[word] = docDict.get(word, 0) + 1
        #If there is at least one query word present in doc
        if isWordP:
            qLen = len(self.searchStrWrdsMap)
            qVector = []
            docVector = []
            #Normalize document word vector by dividing it with totalwordCount in case of web content
            #If word[] is of link or anchor text(title) then divide it by total query words to normalize
            #Normalize query word vector by dividing it by total query words
            for qWord in self.searchStrWrdsMap:
                qVector.append(self.searchStrWrdsMap[qWord]/qLen)
                if isText:
                    docDict[qWord] = docDict.get(qWord, 0)/ttlDocWords
                else:
                    docDict[qWord] = docDict.get(qWord, 0) / qLen
                docVector.append(docDict[qWord])
            priority = 1 - spatial.distance.cosine(qVector, docVector)
        else:
            priority = 0
        return priority

    def writeStatistics(self, ttlTime):
        self.outputFile.write("\n\nTotal number of files visited %d\n" % (len(self.visitedURL)))
        self.outputFile.write("Total Number of 403 Errors %d\n" % (self.count403))
        self.outputFile.write("Total Number of 404 Errors %d\n" % (self.count404))
        self.outputFile.write("Total read data size %f\n" % (self.ttlSize))
        self.outputFile.write("Total time taken %s\n" % (ttlTime))



if __name__ == "__main__":
    # searchStr = "emperor penguin"
    # # searchStr = "duke flatbush"

    searchStr = input("Please enter query to be searched ")
    n = input("Please enter number of pages to be crawled ")
    focused = input("Enter True if focused crawler else False ")
    if focused == "True":
        Crawler = "Focused"
    else:
        Crawler = "BFS"
    try:
        n = int (n)
        print("Search Query is %s Pages to be crawled %d Crawling Type %s" % (searchStr, n, Crawler))
    except:
        print("Provide valid input. Enter number for pages to be crawled")
        sys.exit(1)
    starttime = datetime.now()
    wc = webCrawler(searchStr, n, focused, debug=1, )
    endtime = datetime.now()
    print (endtime - starttime)
    wc.writeStatistics(endtime - starttime)
    wc.outputFile.close()



