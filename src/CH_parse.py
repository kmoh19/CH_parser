myfile='/home/kmoh19/Downloads/Accounts_Bulk_Data-2016-11-02'

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
    recArr=[]
    recString=''
    
    for record in txtpl:
        for tag in record:
            if tag[2]!=None and tag[2]!='' and (tag[0]=='Ukn Conv' or tag[0]=='No attr') and len(tag[2].split())>5: 
                recArr.append(tag[2].lower())
            if tag[3]!=None and tag[3]!='' and (tag[0]=='Ukn Conv' or tag[0]=='No attr') and len(tag[3].split())>5:
                recArr.append(tag[3].lower())
        recString=' '.join(recArr)
    return recString     
    
def HTF(sqn):
    from pyspark.mllib.feature import HashingTF
    word_dict={}
    
    hashingTF=HashingTF(20000)
    tf=hashingTF.transform(sqn.split(' '))
    # do some cleansing
    
    #for term in terms:
        #global term_hash
        #term_hash[term]=hashingTF.indexOf(term)
    
    for word in sqn.split(' '):
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
    clusters= KMeans.train(sparv, 5,maxIterations=10,runs=10) #train the model first pass
    cls=clusters.predict(sparv) # get cluster for document

    return cls,clusters.clusterCenters
            
    


def sparkjob():
    from pyspark import SparkContext,SparkConf
    import numpy
    sparkConf = SparkConf().setAppName("CH_parser")
    sc = SparkContext(conf = sparkConf)
    xml=sc.wholeTextFiles(myfile, minPartitions=4,use_unicode=False)
    #print xml.take(3)
    dbCH=xml.mapValues(CH_parse)
    dbCH_text=dbCH.mapValues(text_get)
    dbCH_seq=dbCH_text.mapValues(seq)
    dbCH_filter=dbCH_seq.filter(lambda x: len(x[1])!=0)
    dbCH_HTF=dbCH_filter.mapValues(HTF)
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
    
    