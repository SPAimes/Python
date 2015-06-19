# -*- coding: utf-8 -*-
import nltk
import pymongo
import _mysql
import sys
import uuid

def ner(inputString, firstName, lastName, userId, feedId, taskId, objectiveId):
    exampleArray = ['Met with Jon Smith from Shell England']
    exampleArray1 = ['Maximise opportunities with Biocity and arrange first meeting in March with Robert Helopin']
    exampleArray2 = ['Good demo with CMO at Barnes & Thornburg in US. He thinks OM could breathe life into key client programme & support their more developed Sector programme. COO likes OM for PDPs, so we could get everything! More to do, but looking good thus far.']
    exampleArray3 = ['Instructed to advise Tesco on the financing of acquisition of Shell group. Tax and litigation (sanctions) teams involved']
    #print inputString 
    #print "na" + firstName
    
    for item in inputString:
        try:
            tokenized = nltk.word_tokenize(item)
            tagged = nltk.pos_tag(tokenized)
          #  print tagged

    
            namedEnt = nltk.ne_chunk(tagged, binary=False)
            for i in range(len(namedEnt)):
                if "ORGANIZATION" in str(namedEnt[i]):
                    print namedEnt[i].label() and namedEnt[i] and namedEnt[i]
                    #connect_MongoDb_Create_Org_Person(namedEnt[i], firstName, lastName, userId, feedId, taskId, objectiveId, inputString)
                    Upsert_Org_Person(namedEnt[i], firstName, lastName, userId, feedId, taskId, objectiveId, inputString)                    
                elif "PERSON" in str(namedEnt[i]):
                    print namedEnt[i].label() and namedEnt[i]
                    
        except "Error NTLK", e:
      
            print "Error %d: %s" % (e.args[0],e.args[1])
            
      #  namedEnt.draw()

def connect_MongoDb_Create_Org_Person(orgName, firstName, lastName, userId, feedId, taskId, objectiveId, feedEntry):
    from pymongo import MongoClient
    client = MongoClient()
    db = client.local
    collection = db.Organization
    orgName = "shane1"
    result = collection.find_one({"orgName": orgName})
    print result
    if result is None:
        print "Creating " and orgName
        db.Organization.insert(
          {
            "_id" : uuid.uuid4(),
            "orgName" : orgName,
            "isPlan" : "false",
            "Person" : {
                "uid" : userId,
                "firstName" : firstName,
                "lastName" : lastName,
                "objectiveId" : objectiveId,
                "feedId" : feedId,
                "feedEntry": feedEntry,
                "taskId" : taskId
            }
        })
    else:
        print "result"
     #   db.Organization.update(
      #    {
       #     "orgName": orgName,
        #    "Person" : {
         #       "uid" : userId,
          #      "firstName" : firstName,
           #     "lastName" : lastName,
            #    "objectiveId" : objectiveId,
             #   "feedId" : feedId,
              #  "feedEntry": feedEntry,
               # "taskId" : "yoohoo"
            #},
            #{ upsert: true }
         # })

        

def connectMySQL():
    import MySQLdb as mdb
    import sys

    try:
        con = mdb.connect('127.0.0.1', 'objectivemanager', 'password', 'objectivemanager')
        cur = con.cursor()
        cur.execute("select * from feed limit 100,20")

        rows = cur.fetchall()

        for row in rows:
                feedString = row[2].decode('latin1')
                #print row
                #print "%s %s" % (feedString, row[10])
                userId = row[10]
                feedId = row[1]
                taskId = ""
                objectiveId = row[3]
                # ner([row[2]])
                userCur = con.cursor()
                sqlString = "select first_name, last_name from user where uid = " + "'" + row[10] + "'"
                #print sqlString
                userCur.execute(sqlString)
                userRow = userCur.fetchone()
                if userRow is not None:
                    firstName = userRow[0]
                    lastName = userRow[1]
                    print firstName + " " + lastName + " " + userId
                    ner([feedString], firstName, lastName, userId, feedId, taskId, objectiveId)
                
        
    
    except mdb.Error, e:
      
        print "Error db %d: %s" % (e.args[0],e.args[1])
        sys.exit(1)
        
    finally:    
            
        if con:    
            con.close()


def Upsert_Org_Person(orgName, firstName, lastName, userId, feedId, taskId, objectiveId, feedEntry):
    import MySQLdb as mdb
    import sys

    try:
        con = mdb.connect('127.0.0.1', 'objectivemanager', 'password', 'objectivemanager')
        cur = con.cursor()
        strId = ""
        strId+= str(uuid.uuid4())
        strOrgName = str(orgName)
        print "Org is " + strOrgName[14:-5]
        strTaskId = "NULL"
                
       # Data Insert into the table
        query = "INSERT INTO ner_org_person (uid, orgName, isPlan, userIsOnPlan, userId, firstName, lastName, objectiveId, feedId, feedEntry, taskId) VALUES" \
        " (" + "'" + strId + "'" + " ," \
            " " + "'" + strOrgName[14:-5] + "'" + "," \
            " " + "'" + "0" + "'" + "," \
            " " + "'" + "0" + "'" + "," \
            " " + "'" + str(userId) + "'" + "," \
            " " + "'" + str(firstName) + "'" + "," \
            " " + "'" + str(lastName) + "'" + "," \
            " " + "'" + str(objectiveId) + "'" + "," \
            " " + "'" + str(feedId) + "'" + "," \
            " " + "'" + str(feedEntry)[3:-2] + "'" + "," \
            " " + "'" + strTaskId + "'" + ")"
        print query
        print str(feedEntry)[3:-2]
        #con.query(query)
        cur.execute(query)
        #con.insert(query)
        con.commit()        
        
    
    except mdb.Error, e:
      
        print "Error db %d: %s" % (e.args[0],e.args[1])
        sys.exit(1)
        
    finally:    
            
        if con:    
            con.close()            

#connectMongoDb("shane Org1")
connectMySQL()
#ner(["For the first time in forever"])
