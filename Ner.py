# -*- coding: utf-8 -*-
import nltk
import pymongo
import _mysql
import sys
import uuid
import re

def ner(inputString, firstName, lastName, userId, feedId, taskId, objectiveId, companyId):
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
            found = "false"

            namedEnt = nltk.ne_chunk(tagged, binary=False)
            for i in range(len(namedEnt)):
                if "ORGANIZATION" in str(namedEnt[i]):
                    print namedEnt[i].label() and namedEnt[i] and namedEnt[i]
                    #connect_MongoDb_Create_Org_Person(namedEnt[i], firstName, lastName, userId, feedId, taskId, objectiveId, inputString)
                    
                    # Let's check if the company name is in the exclusions
                    lines = [line.rstrip() for line in open('NER Exclusions')]
                    for n in lines:
                        strOrgName = str(namedEnt[i])
                        strCleansedOrgName = strOrgName[14:-5]
                        #strCleansedOrgName = re.sub("\NNP", '', strCleansedOrgName)
                        print " comparing " + str(n) + " " + strCleansedOrgName
                        if n == strCleansedOrgName:
                            found = "true"
                            print "Set found to " + found
                            break        

                    if found == "false":            
                        Upsert_Org_Person(namedEnt[i], firstName, lastName, userId, feedId, taskId, objectiveId, inputString, companyId)                    
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
        print "result found it"
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
        cur.execute("select uid, user_added_by, objective_id, entry from feed")

        rows = cur.fetchall()

        for row in rows:
                feedString = row[3].decode('latin1')
                #print row
                #print "%s %s" % (feedString, row[10])
                userId = str(row[1])
                #print userId
                feedId = row[0]
                taskId = ""
                objectiveId = row[2]
                # ner([row[2]])
                userCur = con.cursor()
                sqlString = "select first_name, last_name, company_id from user where uid = " + "'" + userId + "'"
                #print sqlString
                userCur.execute(sqlString)
                userRow = userCur.fetchone()
                if userRow is not None:
                    firstName = userRow[0]
                    lastName = userRow[1]
                    companyId = userRow[2]
                    print firstName + " " + lastName + " " + userId + " " + companyId
                    ner([feedString], firstName, lastName, userId, feedId, taskId, objectiveId, companyId)
                
        
    
    except mdb.Error, e:
      
        print "Error db %d: %s" % (e.args[0],e.args[1])
        sys.exit(1)
        
    finally:    
            
        if con:    
            con.close()


def Upsert_Org_Person(orgName, firstName, lastName, userId, feedId, taskId, objectiveId, feedEntry, companyId):
    import MySQLdb as mdb
    import sys
    import re

    try:
        con = mdb.connect('127.0.0.1', 'objectivemanager', 'password', 'objectivemanager')
        cur = con.cursor()

        strOrgName = str(orgName)
        strCleansedOrgName = re.sub("'/NNP", '', strOrgName[14:-5])
        strCleansedOrgName = re.sub("'", '', strCleansedOrgName)
        intIsPlan = 0

        osCur = con.cursor()
        sqlString = "select uid, user_id from objective_sheet where name like " + "'%" + strCleansedOrgName + "%'" + " and supplier_id = " + "'" + str(companyId) + "'"
        #print sqlString
        osCur.execute(sqlString)
        osRow = osCur.fetchone()
        intUserIsOnPlan = 0
        
        if osRow is not None:
            intIsPlan = 1
            if str(osRow[1]) == str(userId):
                intUserIsOnPlan = 1
            else:            
                osUid = osRow[0]
                ptCur = con.cursor()
                sqlString = "select id from plan_team_user where plan_id = " + "'" + osUid + "'"
                #print sqlString
                ptCur.execute(sqlString)
                ptRow = ptCur.fetchone()
                if ptRow is not None:
                    intUserIsOnPlan = 1        

        userCur = con.cursor()
        sqlString = "select uid from ner_org_person where orgName = " + "'" + strCleansedOrgName + "'" + " and userId = " + "'" + str(userId) + "'" + " and feedId = " + "'" + str(feedId) + "'"
        print sqlString
        userCur.execute(sqlString)
        userRow = userCur.fetchone()
        print str(userRow) + " was the result"
        
        strFeedEntry = re.sub("[!@#'&$]", '', str(feedEntry))
        strFeedEntry = re.sub("'\'", '', strFeedEntry)
         #= feedEntry.translate(None, "'")
        strFirstName = re.sub("'", '', str(firstName))
        strLastName = re.sub("'", '', str(lastName))
        
        if userRow is None:
            
            strId = ""
            strId+= str(uuid.uuid4())
            
            print "Org is " + strOrgName[14:-5]
            strTaskId = "NULL"
                    
           # Data Insert into the table
            query = "INSERT INTO ner_org_person (uid, orgName, isPlan, userIsOnPlan, userId, firstName, lastName, objectiveId, feedId, feedEntry, taskId, companyId) VALUES" \
            " (" + "'" + strId + "'" + " ," \
                " " + "'" + strCleansedOrgName + "'" + "," \
                " " + "'" + str(intIsPlan) + "'" + "," \
                " " + "'" + str(intUserIsOnPlan) + "'" + "," \
                " " + "'" + str(userId) + "'" + "," \
                " " + "'" + strFirstName + "'" + "," \
                " " + "'" + strLastName + "'" + "," \
                " " + "'" + str(objectiveId) + "'" + "," \
                " " + "'" + str(feedId) + "'" + "," \
                " " + "'" + strFeedEntry[2:-2] + "'" + "," \
                " " + "'" + strTaskId + "'" + "," \
                " " + "'" + str(companyId) + "'" + ")"
            print str(query) + " about the insert the record"
            print str(feedEntry)[3:-2]
            #con.query(query)
            cur.execute(query)
            #con.insert(query)
            con.commit()
         
        
    
    except mdb.Error, e:
      
        print "Error db %d: %s" % (e.args[0],e.args[1])
        #sys.exit(1)
        f = open("ner errors.txt","a") #opens file with name of "test.txt"
        f.write("Error db %d: %s" % (e.args[0],e.args[1]))
        f.write(strFeedEntry + "\n")
        f.close() 
        
    finally:    
            
        if con:    
            con.close()            

#connectMongoDb("shane Org1")
#lines = tuple(open('NER Exclusions', 'r'))

connectMySQL()
#ner(["For the first time in forever"])
