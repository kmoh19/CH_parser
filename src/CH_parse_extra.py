myfile='/home/kmoh19/Downloads/Accounts_Bulk_Data-2016-11-02'#'/ch_acct'#
stpwrd=["a", "about", "above", "above", "across", "after", "afterwards", "again", "against", "all", "almost", "alone", "along", "already", "also","although","always","am","among", "amongst", "amoungst", "amount",  "an", "and", "another", "any","anyhow","anyone","anything","anyway", "anywhere", "are", "around", "as",  "at", "back","be","became", "because","become","becomes", "becoming", "been", "before", "beforehand", "behind", "being", "below", "beside", "besides", "between", "beyond", "bill", "both", "bottom","but", "by", "call", "can", "cannot", "cant", "co", "con", "could", "couldnt", "cry", "de", "describe", "detail", "do", "done", "down", "due", "during", "each", "eg", "eight", "either", "eleven","else", "elsewhere", "empty", "enough", "etc", "even", "ever", "every", "everyone", "everything", "everywhere", "except", "few", "fifteen", "fify", "fill", "find", "fire", "first", "five", "for", "former", "formerly", "forty", "found", "four", "from", "front", "full", "further", "get", "give", "go", "had", "has", "hasnt", "have", "he", "hence", "her", "here", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "him", "himself", "his", "how", "however", "hundred", "ie", "if", "in", "inc", "indeed", "interest", "into", "is", "it", "its", "itself", "keep", "last", "latter", "latterly", "least", "less", "ltd", "made", "many", "may", "me", "meanwhile", "might", "mill", "mine", "more", "moreover", "most", "mostly", "move", "much", "must", "my", "myself", "name", "namely", "neither", "never", "nevertheless", "next", "nine", "no", "nobody", "none", "noone", "nor", "not", "nothing", "now", "nowhere", "of", "off", "often", "on", "once", "one", "only", "onto", "or", "other", "others", "otherwise", "our", "ours", "ourselves", "out", "over", "own","part", "per", "perhaps", "please", "put", "rather", "re", "same", "see", "seem", "seemed", "seeming", "seems", "serious", "several", "she", "should", "show", "side", "since", "sincere", "six", "sixty", "so", "some", "somehow", "someone", "something", "sometime", "sometimes", "somewhere", "still", "such", "system", "take", "ten", "than", "that", "the", "their", "them", "themselves", "then", "thence", "there", "thereafter", "thereby", "therefore", "therein", "thereupon", "these", "they", "thickv", "thin", "third", "this", "those", "though", "three", "through", "throughout", "thru", "thus", "to", "together", "too", "top", "toward", "towards", "twelve", "twenty", "two", "un", "under", "until", "up", "upon", "us", "very", "via", "was", "we", "well", "were", "what", "whatever", "when", "whence", "whenever", "where", "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever", "whether", "which", "while", "whither", "who", "whoever", "whole", "whom", "whose", "why", "will", "with", "within", "without", "would", "yet", "you", "your", "yours", "yourself", "yourselves", "the"]

def CH_parse(docLink):
    from lxml import etree
    import re
    
    docTuple=[]
    
    try:
        docTag = etree.fromstring(docLink)
    except etree.XMLSyntaxError,detail:
        return detail.error_log
    
    docTuple.append(docTag.nsmap)
    
    for child in (docTag.getiterator()):
        if (child.text!=None or child.tail!=None) and re.search('^.*STYLE',(etree.tostring(child)).upper())==None:
            
            if child.text==None:
                text=child.text
            else:
                text=(child.text).strip()
            
            if child.tail==None:    
                tail=child.tail
            else: 
                tail=(child.tail).strip()
            
            docTuple.append([child.items(),text,tail])
            #clear text and tuple?
        else:
            continue
                

    return [docTuple]    


def text_get(dbCH_unpk):
    outList=[]
    outDb=[]
    name='Ukn Conv'
    context='Ukn Conv'
    for record in dbCH_unpk:
        for itag in [tag for tag in record if type(tag)!=dict]:
            if len(itag[0])>0:
                for tpl in itag[0]:
                    if tpl[0].upper()=='NAME':
                        name=tpl[1]
                    if tpl[0].upper()=='CONTEXTREF':
                        context=tpl[1]       
                outList.append((name,context,itag[1],itag[2]))
                name='Ukn Conv'
                context='Ukn Conv'
            else:
                outList.append(('No attr','No ctx',itag[1],itag[2]))
        outDb.append(outList)
        outList=[]
    return outDb


def seq(txtpl):
    import re
    recArr=[]
    recArr2=[]
    recString=''
    
    for record in txtpl:
        for tag in record:
            if tag[2]!=None and tag[2]!='' and (tag[0]=='Ukn Conv' or tag[0]=='No attr') and len(tag[2].split())>5: 
                recArr.append(tag[2].lower())
            if tag[3]!=None and tag[3]!='' and (tag[0]=='Ukn Conv' or tag[0]=='No attr') and len(tag[3].split())>5:
                recArr.append(tag[3].lower())
        #print recArr
        recArr2= [x for x in re.findall(r'\b\w+\b',' '.join(recArr)) if x not in stpwrd]
        recString=' '.join(recArr2)
        #print recString
    return recString

def ngramIt(sqn,n=5):
    sqn=sqn.split()
    output=[]
    for i in range(len(sqn)-n+1):
        output.append(' '.join(sqn[i:i+n]))    
        #print ' '.join(sqn[i:i+n])
    #print ','.join(output)
    return ','.join(output) #ngrams('a b c d', 2) # [['a', 'b'], ['b', 'c'], ['c', 'd']]
    
def HTF(sqn):
    from pyspark.mllib.feature import HashingTF
    word_dict={}
    
    hashingTF=HashingTF(100000)
    tf=hashingTF.transform(sqn.split(','))
    # do some cleansing
    
    #for term in terms:
        #global term_hash
        #term_hash[term]=hashingTF.indexOf(term)
    
    for word in sqn.split(','):
        word_dict[word]=hashingTF.indexOf(word)
        
    #print sqn,len(sqn.split(' '))  

    return (tf,word_dict)            
    
def iDF(TF):
    from pyspark.mllib.feature import IDF
    idf=IDF(minDocFreq=5).fit(TF)
    tfidf=idf.transform(TF)

    return tfidf

def cluster(sparv):
    from pyspark.mllib.clustering import KMeans #,KMeansModel
    clusters= KMeans.train(sparv, k=8,maxIterations=100,runs=1) #train the model first pass
    cls=clusters.predict(sparv) # get cluster for document

    return cls,clusters.clusterCenters
            
            
    

def sparkjob():
    from pyspark import SparkContext,SparkConf
    import numpy
    sparkConf = SparkConf().setAppName("CH_parser").set('spark.memory.fraction', 0.2)
    sc = SparkContext(conf = sparkConf)
    xml=sc.wholeTextFiles(myfile, minPartitions=4,use_unicode=False)
    #print xml.take(3)
    dbCH=xml.mapValues(CH_parse)
    dbCH_text=dbCH.mapValues(text_get)
    dbCH_seq=dbCH_text.mapValues(seq)
    dbCH_filter=dbCH_seq.filter(lambda x: len(x[1])!=0)
    dbCH_ngram=dbCH_filter.mapValues(ngramIt)
    dbCH_HTF=dbCH_ngram.mapValues(HTF)
    dbCH_HTF.cache()
    dbCH_Tfidf=iDF(dbCH_HTF.map(lambda x: x[1][0]))#dbCH_HTF.mapValues(iDF)
    dbCH_Final=dbCH_HTF.keys().zip(dbCH_Tfidf)
    dbCH_Final.cache()
    
    Kmodel,Ccentre=cluster(dbCH_Final.values())
    
    dbCH_FinalKm=dbCH_Final.zip(Kmodel).zip(dbCH_HTF.map(lambda x: x[1][1]))

    dbCH_Dic=dbCH_FinalKm.map(lambda x: x[1]).reduce(lambda d0,d1: d0.update(d1) or d0)
    
    Dic={v:k for k,v in dbCH_Dic.iteritems()}
    
    dbCH_col=dbCH_FinalKm.collect()

    Cluster_Centres=[]
    for a in Ccentre:
        Cluster_Centres.append({Dic[i]:a[i] for i in numpy.nonzero(a)[0]})
        print {Dic[i]:a[i] for i in numpy.nonzero(a)[0]}
    #Cluster_Centres2=[]    
    #for centre in Cluster_Centres:
        #for key in centre.keys():
            #Dic[key]:key
        
    
    
    
    #print len(dbCH_Dic),dbCH_Dic


    #for i in dbCH_col:
        #print i[0][0][0],i[0][1],i[1]
    
    #for i in Kmodel:
        #for j in i:
            #print j
        #print '##################################################################################'
    
    sc.stop()
    
    return 'done!'

sparkjob()




#((('file:/home/kmoh19/Downloads/Accounts_Bulk_Data-2016-11-02/Prod223_1734_04923104_20161031.html', SparseVector(20000, {868: 0.6847, 2050: 1.139, 3585: 0.5688, 4110: 0.669, 4561: 0.6231, 4717: 1.1654, 5146: 7.9235, 5157: 7.5181, 5855: 4.4735, 6951: 1.4962, 7060: 0.3256, 8289: 0.4943, 8297: 2.1853, 9382: 1.0761, 10427: 1.2733, 10449: 1.2075, 11034: 1.9656, 11824: 1.5043, 12547: 4.1508, 13130: 1.3257, 13207: 1.6037, 13937: 1.1123, 15774: 0.669, 17324: 0.1232, 18176: 0.8643, 18608: 7.9235, 18922: 1.7099, 18954: 0.6875})), 0), {'and': 18176, 'limited': 3585, 'abbreviated': 2050, 'meadow': 5157, 'period': 4717, 'ended': 10449, 'accounts': 7060, 'measurement': 13207, 'for': 9382, 'their': 4561, '-': 17324, 'to': 13937, '(management': 5146, '2016': 10427, 'company': 18954, 'services)': 18608, 'behalf': 15774, 'on': 8289, 'october': 12547, 'by:': 868, 'of': 8297, '31': 18922, 'basis': 6951, 'signed': 4110, 'preparation': 11824, 'the': 11034, 'notes': 13130, 'view': 5855})
    
    