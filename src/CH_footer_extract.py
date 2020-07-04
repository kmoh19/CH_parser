folders=[folder_path0,folder_path1,folder_path2,folder_path3,folder_path4,folder_path5,folder_path6,folder_path7,folder_path8,folder_path9,folder_path10,folder_path11,folder_path12,folder_path13,folder_path14,folder_path15,folder_path16,folder_path17,folder_path18,folder_path19,folder_path20,folder_path21,folder_path22]



def parse_xml(xml_string0):
    from lxml import etree
    from StringIO import StringIO
    import base64
    import re
    
    xml_string=re.sub(r'http://www.govtalk.gov.uk/taxation/CT/\d+','http://www.govtalk.gov.uk/taxation/CT/X',xml_string0)
    ns = {}
    ns["cm"]="http://www.govtalk.gov.uk/CM/envelope"
    ns["su"]="http://www.govtalk.gov.uk/gateway/submitterdetails"
    ns["ct"]="http://www.govtalk.gov.uk/taxation/CT/X"
    

    def ex_elements(xml, xpath, name):
        return xml.xpath(xpath,namespaces=name)

    def extract_tags(doc, node, ns):
        docs = ex_elements(doc, node, ns)
        docs_tags = []
        for doc in docs:
            attached_files = ex_elements(doc, 'ct:Instance/*', ns)
            for f in attached_files:
                enc_ixbrl =(ex_elements(doc,'//ct:IRmark', ns)[0].text, f.text)
                docs_tags.append(enc_ixbrl)
        return docs_tags

    try:
        doc = etree.parse(StringIO(xml_string))

        ixbrl = []

        comps = extract_tags(doc, '//ct:Computation', ns)
        #accts = extract_tags(doc, '//ct:Accounts', ns)
        for comp in comps:
            #if cmpl.search(comp.strip())!=None
           # cmpl=re.compile(r'^([A-Za-z0-9+/\]{4})*([A-Za-z0-9+/\]{4}|[A-Za-z0-9+/\]{3}=|[A-Za-z0-9+/\]{2}==)$')
            ixbrl.append(comp)
            #else:
             #   ixbrl.append(base64.b64encode(comp))

        #for acct in accts:
         #   if cmpl.search(acct)!=None:
          #      ixbrl.append(acct)
           # else:
            #    ixbrl.append(base64.b64encode(acct))

        return ixbrl
    except:
       return [('0','0')]




def CH_parse2(ixbrl):
    from lxml import etree
    import re

    docTuple=[]

    try:
        docTag = etree.fromstring(ixbrl)
    except etree.XMLSyntaxError,detail:
        return ['Fail']

    docTuple.append(docTag.nsmap)

    for child in (docTag.getiterator()):
        if (child.text!=None or child.tail!=None) and re.search('^[<\s]{1,2}STYLE',(etree.tostring(child)).upper())==None and child.attrib.has_key('class') and re.search(r'.*foot.*',child.get('class')):

            fchild = child.xpath('descendant-or-self::*')
            for ichild in fchild:

                if ichild.text==None:
                    text=ichild.text
                else:
                    text=(ichild.text).strip()

                if ichild.tail==None:
                    tail=ichild.tail
                else:
                    tail=(ichild.tail).strip()

                docTuple.append([ichild.items(),text,tail])
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
    
    for record in txtpl:# tag and untagged being run revisit for discussion
        for tag in record:
            if tag[2]!=None and tag[2]!='' and len(tag[2].split())>7: #text
                recArr.append(tag[2].lower())
            if tag[3]!=None and tag[3]!='' and len(tag[3].split())>7: #tail
                recArr.append(tag[3].lower())
        #print recArr
        recArr2= [x for x in re.findall(r'\b[a-zA-Z_]+\b',' '.join(list(recArr)))]# if x not in stpwrd]#remove numbers and 
        recString=' '.join(recArr2)
        #print recString
    return recString



def ngramIt2(sqn,n=15):
    sqn=sqn.split()
    output=[]
    for i in range(len(sqn)-n+1):
        output.append(' '.join(sqn[i:i+n]))
        #print ' '.join(sqn[i:i+n])
    #print ','.join(output)
    return ','.join(list(set(output))) #ngrams('a b c d', 2) # [['a', 'b'], ['b', 'c'], ['c', 'd']]

def ngramIt(sqn,n=15):
    output=[' '.join(sqn.split()[i:i+n]) for i in range(0, len(sqn.split()), n)]
    output2=[x for x in output if len(x.split())==n]
    return ','.join(list(set(output2)))



def sparkjob():
    from pyspark import SparkContext,SparkConf
    import re
    import base64
    sparkConf = SparkConf().setAppName("CH_parser").set('spark.memory.fraction',0.6).set('spark.kryoserializer.buffer.max.mb','222400')
    sc = SparkContext('yarn-client',conf = sparkConf)
    
    def phraser(xml):
        b64=xml.flatMapValues(parse_xml).filter(lambda (k,v): v[1]!=None and v[1]!='0')
        b64parse=b64.mapValues(lambda x: (x[0],base64.b64decode (x[1])))
        dbCH=b64parse.mapValues(lambda x: (x[0],CH_parse2(x[1]))).filter(lambda (k,v): v[1][0]!='Fail')
        dbCH_text=dbCH.mapValues(lambda x: (x[0],text_get(x[1])))
        dbCH_seq=dbCH_text.mapValues(lambda x:(x[0],seq(x[1])))
        dbCH_filter=dbCH_seq.filter(lambda x: len(x[1][1])!=0)
        dbCH_ngram=dbCH_filter.mapValues(lambda x: (x[0],ngramIt(x[1])))
        dbCH_fngram=dbCH_ngram.map(lambda (a,(k,v)): ((a,k),v)).flatMapValues(lambda line:line.split(',')).mapValues(lambda word:(word,1)).map(lambda(k,v):(v[0],(k[1],v[1]))).reduceByKey(lambda(a,b),(c,d):(','.join([a,c]),b+d))
        #dbCH_HTF.cache()
        return dbCH_fngram
    
    xml_rdds=[]
    for folder in folders:
        xml=sc.wholeTextFiles(folder, minPartitions=200,use_unicode=False)
        xml_rdds.append(xml)
    
    x_of_15=[]
    for xml_rdd in xml_rdds:
        a=phraser(xml_rdd)
        x_of_15.append(a)
        
    
    all_15 = sc.union(x_of_15).reduceByKey(lambda(a,b),(c,d):(','.join([a,c]),b+d)).filter(lambda x:x[1][1]>14 and x[1][1]<200)
    all_15.cache()
    taly=all_15.count()
    all_15.saveAsTextFile('/user/7830250/ris_footer_23_subfix_IR')
    

    
    #dbCH_Dic=dbCH_FinalKm.map(lambda x: x[1]).reduce(lambda d0,d1: d0.update(d1) or d0)
    
    #Dic={v:k for k,v in dbCH_Dic.iteritems()}
    
   # dbCH_col=all_15
    print taly
    print 'all done!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!'
    #for i in dbCH_col:
     #   print i
        
    
    sc.stop()
    
    return 'done!'

#sparkjob()

def sparkjoba():
    from pyspark import SparkContext,SparkConf
    import re
    import base64
    sparkConf = SparkConf().setAppName("CH_phr_frq").set('spark.memory.fraction', 0.6).set('spark.kryoserializer.buffer.max','1024')
    sc = SparkContext('yarn-client',conf = sparkConf)
    db=sc.wholeTextFiles('/user/7830250/ris_footer_23_subfix_IR', minPartitions=200,use_unicode=False)
    db1=db.flatMapValues(lambda x:x.split('\n')).filter(lambda x: len(x[1])>5).map(lambda x:(re.findall(r"'([a-z_\s]*)'",x[1])[0],int(re.findall(r", ([0-9]+)",x[1])[0]))).map(lambda x: (x[1],1)).reduceByKey(lambda a,b:a+b).collect()
    for i in db1:
        print i
    print len(db1)
    return 'done'

#sparkjoba()

def sparkjobb():
    from pyspark import SparkContext,SparkConf
    import re
    import base64
    from ast import literal_eval
    sparkConf = SparkConf().setAppName("CH_phr_frq").set('spark.memory.fraction', 0.6).set('spark.kryoserializer.buffer.max','1024')
    sc = SparkContext('yarn-client',conf = sparkConf)
    db=sc.wholeTextFiles('/user/7830250/ris_footer_23_subfix_IR', minPartitions=200,use_unicode=False)
    db1=db.flatMapValues(lambda x:x.split('\n')).filter(lambda x: len(x[1])>5).mapValues(literal_eval).map(lambda x: (x[1][0],x[1][1][1])).collect()
    for i in db1:
        print i
    print len(db1)
    return 'done'

#sparkjobb()



def sparkjobc():
    from pyspark import SparkContext,SparkConf
    import re
    import base64
    from ast import literal_eval
    sparkConf = SparkConf().setAppName("CH_phr_frq").set('spark.memory.fraction', 0.6).set('spark.kryoserializer.buffer.max','1024')
    sc = SparkContext('yarn-client',conf = sparkConf)
    db=sc.wholeTextFiles('/user/7830250/ris_footer_23_subfix_IR', minPartitions=200,use_unicode=False)
    db1=db.flatMapValues(lambda x:x.split('\n')).filter(lambda x: len(x[1])>5).mapValues(literal_eval).map(lambda x: (x[1][0],x[1][1][0])).flatMapValues(lambda x:x.split(',')).collect()
    for i in db1:
        print i
    print len(db1)
    return 'done'
 
#sparkjobc()


def sparkjobd():
    from pyspark import SparkContext,SparkConf
    import re
    import base64
    from ast import literal_eval
    sparkConf = SparkConf().setAppName("CH_phr_frq").set('spark.memory.fraction', 0.6).set('spark.kryoserializer.buffer.max','1024')
    sc = SparkContext('yarn-client',conf = sparkConf)
    db=sc.textFile('/user/7829441/index.txt', minPartitions=200,use_unicode=False)
    db1=db.map(literal_eval).map(lambda x:x['irmark']).take(10)
    for i in db1:
        print i
    print len(db1)
    return 'done'
 
sparkjobd()

