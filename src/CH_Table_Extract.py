
folders=[folder_path0,folder_path1,folder_path2,folder_path3,folder_path4,folder_path5,folder_path6,folder_path7,folder_path8,folder_path9,folder_path10,folder_path11,folder_path12,folder_path13,folder_path14,folder_path15,folder_path16,folder_path17,folder_path18,folder_path19,folder_path20,folder_path21,folder_path22]

def parse_xml(xml_string0):
    from lxml import etree
    from StringIO import StringIO
    import re

    xml_string=re.sub(r'http://www.govtalk.gov.uk/taxation/CT/\d+','http://www.govtalk.gov.uk/taxation/CT/X',xml_string0)
    ns = {}
    ns["cm"]="http://www.govtalk.gov.uk/CM/envelope"
    ns["su"]="http://www.govtalk.gov.uk/gateway/submitterdetails"
    ns["ct"]="http://www.govtalk.gov.uk/taxation/CT/X"

    def ex_elements(xml, xpath, name):
        return xml.xpath(xpath,namespaces=name)

    def extract_tags(doc, node, ns):
        """Helper function: Extract iXBRL Documents.

        Returns base 64 iXBRL document.  
        Uses ex_elements to extract the node
        Tests whether the document is correctly encoded.
        If f has a length it means the attachment is not 
        encoded and has been parsed as XML.
        In this case the function extracts the XML and then
        encodes it back into base 64
        """

        docs = ex_elements(doc, node, ns)
        docs_tags = []
        for doc in docs:
            attached_files = ex_elements(doc, 'ct:Instance/*', ns)
            for f in attached_files:
                if not len(f):
                    enc_ixbrl = (
                        f.text
                )
                else:
                    enc_ixbrl = (
                        base64.b64encode(re.sub('\s+',' ',etree.tostring(f[0])))
                    )
                docs_tags.append(enc_ixbrl)
        return docs_tags
    try:
        # Create a parser that can cope with large trees
        p = etree.XMLParser(huge_tree=True)
        doc = etree.parse(StringIO(xml_string), parser=p)

        UTR=ex_elements(doc,'//cm:Key',ns)[0].text
        AP=ex_elements(doc,'//ct:To',ns)[0].text

        ixbrl = []

        comps = extract_tags(doc, '//ct:Computation', ns)
        #accts = extract_tags(doc, '//ct:Accounts', ns)
        for comp in comps:
            ixbrl.append(comp)
        #for acct in accts:
            #ixbrl.append(acct)
        
        return (ixbrl,(UTR,AP),ex_elements(doc,'//ct:IRmark',ns)[0].text)
    except:
        return (['0'],(False,False),'No Parse')


def CH_parse2(ixbrl):
    from lxml import etree
    import re

    docTuple=[]
    try:
     docTag = etree.fromstring(ixbrl)
    except etree.XMLSyntaxError,detail:
     return 'Fail'
    
    li=[]
    li1=[]
    table_data=[]
    table_data1=[]
    itr=list(docTag.getiterator())

    terms = [
            'adjustment of profit',
            'adjustment of loss',
            'adjustment of profit and tax payable',
            'accounts adjustments',
            'calculation of tax liability',
            'adjustment of income and expense statement',
            'adjustment of trading profit',
            'tax reconciliation',
            'losses and allowances',
            'adjustment of income and expense statement',
            'income statement',
            'accounts adjustments'
            ]

    terms = r'|'.join(terms)

    li=[i for i in itr if i.tag=='{http://www.w3.org/1999/xhtml}table' and re.search(terms,' '.join(i.xpath('preceding-sibling::*/descendant-or-self::text()')).lower()) and not re.search(r'content',' '.join(i.xpath('preceding-sibling::*/descendant-or-self::text()')).lower().strip()[0:20])]
    k=0
    for table in li:
        k=k+1
        v=0
        rows=table.findall("{http://www.w3.org/1999/xhtml}tr")
        for rec in rows:
            v=v+1
            cells=rec.findall("{http://www.w3.org/1999/xhtml}td")
            for cell in cells:
                #table_data.append((k,v,' '.join([(i.text or ' ').strip() or (i.tail or ' ').strip() for i in cell.xpath('descendant-or-self::*')])))
                table_data.append((k,v,''.join(cell.xpath('descendant-or-self::text()'))))


    
    li1=[i for i in itr if (i.text or i.tail or 'n/a').lower().strip()=='adjustment of profit']
    for tag in li1:
        table_data1.append([(i.text or ' ').strip() or (i.tail or ' ').strip() for i in tag.xpath('self::*|following-sibling::*/descendant-or-self::*')])

    #def g(x):return len(x[2])>0
    return table_data,table_data1



def sparkjob():
    from pyspark import SparkContext,SparkConf
    from pyspark.sql import SQLContext, Row
    import re
    import base64
    sparkConf = SparkConf().setAppName("CH_parser").set('spark.memory.fraction', 0.6).set('spark.kryoserializer.buffer.max','1024')
    sc = SparkContext('yarn-client',conf = sparkConf)
    sqlContext=SQLContext(sc)
    def phraser(xml):
        b64=xml.mapValues(parse_xml).map(lambda (k,v):((k,v[2],v[1]),v[0])).flatMapValues(lambda x:x).filter(lambda (k,v): v!=None and v!='0')
        b64parse=b64.mapValues(base64.b64decode)
        dbCH=b64parse.mapValues(CH_parse2).filter(lambda (k,v): v!='Fail')
        return dbCH
    
    xml_rdds=[]
    
    EDF=sc.textFile('/user/7830250/20170801LBDA1251UtilitiesUTRs.csv')
    EDF_list=EDF.map(lambda x: x.split(',')).collect()
    EDF_bcast=sc.broadcast([i[0] for i in EDF_list])


    for folder in folders:
        xml=sc.wholeTextFiles(folder, minPartitions=200,use_unicode=False)
        xml_rdds.append(xml)
    
    x_of_15=[]
    for xml_rdd in xml_rdds:
        a=phraser(xml_rdd)
        x_of_15.append(a)
        
    #print EDF_bcast.value
    all_15 = sc.union(x_of_15).filter(lambda x:x[0][2][0] in EDF_bcast.value).collect()
    #output=all_15.map(lambda x: Row(cas=x[0][0],irmark=x[0][1],isCT=bool(x[0][2]),isIX=bool(x[1])))
    #schemaOutput=sqlContext.inferSchema(output)
    #schemaOutput.registerTempTable('output')
    #all_15_q=sqlContext.sql('SELECT cas,irmark,isCT,isIX FROM output').map(lambda x: (x.cas,x.irmark))
    #all_15_q.saveAsTextFile('/user/7830250/LB_CFC_F_T')

    
    #dbCH_col=all_15_q.take(100)
    for i in all_15:
        print i
    #for it in EDF_list:
     #   print it
    #print 'all done!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!'
    #print 'length is:',len(dbCH_col)

        
    
    sc.stop()
    
    return 'done!'

#sparkjob()

def sparkjoba():
    from pyspark import SparkContext,SparkConf
    import re
    import base64
    import numpy as np
    from ast import literal_eval
    sparkConf = SparkConf().setAppName("CH_parser").set('spark.memory.fraction', 0.6).set('spark.kryoserializer.buffer.max','1024')
    sc = SparkContext('yarn-client',conf = sparkConf)

    def cln(a):

        output = ""
        tables = list(set( [x[0] for x in a] ))

        max_rows = max([x[1] for x in a ])
        divider = ("#---------" + "\t")*5

        for indx, table in enumerate(tables):

            b=[[] for y in xrange(0,max_rows)]
            subset = [ x for x in a if x[0] == table]

            for i in subset:
                b[i[1]-1].append(i[2].strip())
            c='\n'.join(['\t'.join(x) for x in b if len (x)>0])
            
            if indx == 0:
                output = output + c
            else:
                output = output + "\n\n\n" + divider + "\n\n\n" + c


        return output

    db=sc.textFile('/user/7828808/Tgap_2')
    db1=db.map(literal_eval).map(lambda x:(x[0][1],x[1][0])).filter(lambda x:len(x[1])>0).mapValues(cln).collect()
    t=0
    for i in db1:
        print i
    #print len(db1)


#sparkjoba()


def prt_tsv():
    from ast import literal_eval

    with open('/home/users/7830250/Emerging_Phrases/testa3') as outFile:
        all_50= outFile.readlines()
        outFile.close()
    t=0
    for i in all_50:
        print t,literal_eval(i)[0]
        with open('/home/users/7830250/Emerging_Phrases/EDF_output4/File_'+str(t)+'.tsv','wb')as outFile:
            print >> outFile,(literal_eval(i)[1]).encode('cp1252')
            outFile.close()
        t=t+1
    return None
    
#prt_tsv()

def prt_csv():
    import csv
    from ast import literal_eval
    with open('tgap.out', 'rb') as csvfile:
        ireader = csv.reader(csvfile,delimiter='\n',quotechar='\'')
        t=0
        for i in ireader:
            print t,literal_eval(i[0])[0]
            with open('/tmp/EDF3/file_'+str(t)+'.csv','wb') as csvfilew:
                iwriter=csv.writer(csvfilew,delimiter=',')
                for line in [x.split('\t') for x in literal_eval(i[0])[1].split('\n')]:
                    iwriter.writerow([x.encode('cp1252','ignore') for x in line])
            csvfilew.close()
            t=t+1

    return None
prt_csv()
