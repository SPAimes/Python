# -*- coding: utf-8 -*-
import nltk
import pymongo
import _mysql
import sys

def ner(inputString):
    exampleArray = ['Met with Jon Smith from Shell England']
    exampleArray1 = ['Maximise opportunities with Biocity and arrange first meeting in March with Robert Helopin']
    exampleArray2 = ['Good demo with CMO at Barnes & Thornburg in US. He thinks OM could breathe life into key client programme & support their more developed Sector programme. COO likes OM for PDPs, so we could get everything! More to do, but looking good thus far.']
    exampleArray3 = ['Instructed to advise Tesco on the financing of acquisition of Shell group. Tax and litigation (sanctions) teams involved']
   # print inputString
    for item in inputString:
        tokenized = nltk.word_tokenize(item)
        tagged = nltk.pos_tag(tokenized)
      #  print tagged

        namedEnt = nltk.ne_chunk(tagged, binary=False)

        try: 
            for i in range(len(namedEnt)):
                if "ORGANIZATION" in str(namedEnt[i]):
                    print namedEnt[i].label() and namedEnt[i]
                elif "PERSON" in str(namedEnt[i]):
                    print namedEnt[i].label() and namedEnt[i]
                    
        except "Error NTLK", e:
      
            print "Error %d: %s" % (e.args[0],e.args[1])
            
      #  namedEnt.draw()

def connectMongoDb():
    from pymongo import MongoClient
    client = MongoClient()
    db = client.local
    collection = db.Organization
    print collection.find_one()

def connectMySQL():
    import MySQLdb as mdb
    import sys

    try:
        con = mdb.connect('127.0.0.1', 'objectivemanager', 'Emoyeni333', 'objectivemanager')
        cur = con.cursor()
        cur.execute("select * from feed limit 100")

        rows = cur.fetchall()

        for row in rows:
            #    print row[2]
                ner([row[2]])
        
    
    except mdb.Error, e:
      
        print "Error %d: %s" % (e.args[0],e.args[1])
        sys.exit(1)
        
    finally:    
            
        if con:    
            con.close()

connectMongoDb()
connectMySQL()
ner(["For the first time in forever"])
