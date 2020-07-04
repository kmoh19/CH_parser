myfile='/home/adminuser/Downloads/Accounts'#'/ch_acct'#

def CH_parse(docLink):
    from lxml import etree
    import re
    
    docTuple=[]
    
    try:
        docTag = etree.fromstring(docLink)
    except etree.XMLSyntaxError:
        return #detail.error_log
    
    docTuple.append(docTag.nsmap)
    
    for child in (docTag.getiterator()):
        if (child.text!=None or child.tail!=None) and re.search('^.*STYLE',(etree.tostring(child).decode('utf-8')).upper())==None:
            
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
            if tag[2]!=None and tag[2]!='' and (tag[0]=='Ukn Conv' or tag[0]=='No attr') and len(tag[2].split())>4: 
                recArr.append(tag[2].lower())
            if tag[3]!=None and tag[3]!='' and (tag[0]=='Ukn Conv' or tag[0]=='No attr') and len(tag[3].split())>4:
                recArr.append(tag[3].lower())
                
        #print recArr
        #recArr2= [x for x in re.findall(r'\b\w+\b',' '.join(recArr))]
        #recString=' '.join(recArr2)
        #print recString
    return recArr

def tag_decn(comtpl):
    from difflib import SequenceMatcher
    result=''
    
    for el in comtpl[0][1]:
        for tpl in el:
            if ((SequenceMatcher(None,comtpl[1][0] or ' ',tpl[2] or '###').ratio()>0.6) or
                (SequenceMatcher(None,comtpl[1][0] or ' ',tpl[3] or '###').ratio()>0.6)) and (tpl[0]!='Ukn Conv' and tpl[0]!='No attr'):
                result=','.join(['tagged',tpl[0],tpl[1]])
                break
            else:
                result='untagged'
    return result
    


def tag_decn2(comtpl):
    import re
    result=''
    
    for el in comtpl[0][1]:
        for tpl in el:
            #print comtpl[1][0],tpl[2] or '#k##',tpl[3] or '#k##' 
            if ((re.search(r'.*'+re.escape(comtpl[1][0])+'.*',(tpl[2] or '#k##').lower())!=None) or
                (re.search(r'.*'+re.escape(comtpl[1][0])+'.*',(tpl[3] or '#k##').lower())!=None)) and (tpl[0]!='Ukn Conv' and tpl[0]!='No attr'):
                result=','.join(['tagged',tpl[0],tpl[1]])
                break
            else:
                result='untagged'
    
    return result

def sparkjob():
    from pyspark import SparkContext,SparkConf
    import re
    sparkConf = SparkConf().setAppName("CH_parser").set('spark.memory.fraction', 0.6)
    sc = SparkContext(conf = sparkConf)
    xml=sc.wholeTextFiles(myfile, minPartitions=4,use_unicode=False)
    #print xml.take(3)
    dbCH=xml.mapValues(CH_parse)
    dbCH_text=dbCH.mapValues(text_get)
    dbCH_seq=dbCH_text.mapValues(seq)
    dbCH_Zip=dbCH_text.zip(dbCH_seq.map(lambda x: x[1]))
    dbCH_filter=dbCH_Zip.filter(lambda x: len(x[1])!=0)
    dbCH_result=dbCH_filter.map(lambda x: (x,tag_decn2(x)))
    
    dbCH_Cols=dbCH_result.filter(lambda x: re.search('^tagged.*', x[1])!=None)
    dbCH_Col=dbCH_Cols.collect()
    for i in dbCH_Col:
        print (i)
 
            
    sc.stop()
    
    return 'done!'

sparkjob()

  
