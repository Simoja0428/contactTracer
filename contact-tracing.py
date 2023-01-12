import sqlite3
from sqlite3 import Error

from flask import Flask
from flask import abort
from flask import request
from flask import redirect

app = Flask(__name__)

@app.route("/person/newPerson", methods = ["POST"])
def addPerson():
    newPerson ={}
    conn = None
    try:
        jsonPostData = request.get_json()
        firstName = jsonPostData["firstName"]
        lastName = jsonPostData["lastName"]
        phoneNum = jsonPostData["phoneNum"]

        conn = sqlite3.connect("contactTracer.db")
        conn.row_factory = sqlite3.Row
        sql = """
            INSERT INTO PERSON (firstName, lastName, phoneNum) VALUES (?,?,?)
        """
        cursor = conn.cursor()
        cursor.execute(sql,(firstName,lastName,phoneNum))
        conn.commit()
        sql = """
            SELECT Person.userId, Person.firstName, Person.lastName, Person.phoneNum
            FROM Person
            WHERE Person.userId = ?
        """
        cursor.execute(sql, (cursor.lastrowid,))
        row = cursor.fetchone()
        newPerson["userId"] = row["userId"]
        newPerson["firstName"] = row["firstName"]
        newPerson["lastName"] = row["lastName"]
        newPerson["phoneNum"] = row["phoneNum"]
    except Error as e:
        print(f"Error opening the database {e}")
        abort(500)
    finally:
        if conn:
            conn.close()

    return newPerson

@app.route("/symptom/newSymptom", methods = ["POST"])
def addSymptom():
    newSymptom = {}
    conn = None
    try:
        jsonPostData = request.get_json()
        symptomName = jsonPostData["symptomName"]

        conn = sqlite3.connect("contactTracer.db")
        conn.row_factory = sqlite3.Row
        sql = """
        INSERT INTO SYMPTOM (symptomName) VALUES (?)
        """
        cursor = conn.cursor()
        cursor.execute(sql, [symptomName])
        conn.commit()
        sql = """
        SELECT Symptom.symptomName, Symptom.symptomId
        FROM Symptom
        WHERE Symptom.symptomId = ?
        """
        cursor.execute(sql, (cursor.lastrowid,))
        row = cursor.fetchone()
        newSymptom["symptomId"] = row["symptomId"]
        newSymptom["symptomName"] = row["symptomName"]
    except Error as e:
        print(f"Error opening the database {e}")
        abort(500)
    finally:
        if conn:
            conn.close()

    return newSymptom

@app.route("/disease/newDisease", methods = ["POST"])
def addDisease():
    newDisease = {}
    conn = None
    try:
        jsonPostData = request.get_json()
        diseaseName = jsonPostData["diseaseName"]

        conn = sqlite3.connect("contactTracer.db")
        conn.row_factory = sqlite3.Row
        sql = """
        INSERT INTO DISEASE (diseaseName) VALUES (?)
        """
        cursor = conn.cursor()
        cursor.execute(sql, [diseaseName])
        conn.commit()
        sql = """
        SELECT Disease.diseaseName, Disease.diseaseId
        FROM Disease
        WHERE Disease.diseaseId = ?
        """
        cursor.execute(sql, (cursor.lastrowid,))
        row = cursor.fetchone()
        newDisease["diseaseId"] = row["diseaseId"]
        newDisease["diseaseName"] = row["diseaseName"]
    except Error as e:
        print(f"Error opening the database {e}")
        abort(500)
    finally:
        if conn:
            conn.close()

    return newDisease

@app.route("/symptom/<int:userId>/personHasSymptom", methods = ["POST"])
def personHasSymptom(userId):
    newSymptomExhibited= {}
    conn = None
    try:
        jsonPostData = request.get_json()
        symptomDate = jsonPostData["symptomDate"]
        symptomId = jsonPostData["symptomId"]

        conn = sqlite3.connect("contactTracer.db")
        conn.row_factory = sqlite3.Row
        sql = """
        INSERT INTO "PERSON HAS SYMPTOM"(symptomId, userId, symptomDate) VALUES (?,?,?)
        """
        cursor = conn.cursor()
        cursor.execute(sql, (symptomId, userId, symptomDate))
        conn.commit()
        sql = """
        SELECT "Person has Symptom".symptomId, "Person has Symptom".userId, "Person has Symptom".symptomDate
        FROM "Person has Symptom"
        WHERE "Person has Symptom".userId = ?
        """
        cursor.execute(sql, (cursor.lastrowid,))
        row = cursor.fetchone()
        newSymptomExhibited["symptomId"] = row["symptomId"]
        newSymptomExhibited["userId"] = row["userId"]
        newSymptomExhibited["symptomDate"] = row["symptomDate"]
    except Error as e:
        print(f"Error opening the database {e}")
        abort(500)
    finally:
        if conn:
            conn.close()

    return newSymptomExhibited

@app.route("/disease/<int:userId>/personHasDisease", methods = ["POST"])
def personHasDisease(userId):
    newDiseaseExhibited= {}
    conn = None
    try:
        jsonPostData = request.get_json()
        positiveDate = jsonPostData["positiveDate"]
        diseaseId = jsonPostData["diseaseId"]

        conn = sqlite3.connect("contactTracer.db")
        conn.row_factory = sqlite3.Row
        sql = """
        INSERT INTO "PERSON TESTED POSITIVE FOR DISEASE"(diseaseId, userId, positiveDate) VALUES (?,?,?)
        """
        cursor = conn.cursor()
        cursor.execute(sql, (diseaseId, userId, positiveDate))
        conn.commit()
        sql = """
        SELECT "Person Tested Positive for Disease".diseaseId, "Person Tested Positive for Disease".userId, "Person Tested Positive for Disease".positiveDate
        FROM "Person Tested Positive for Disease"
        WHERE "Person Tested Positive for Disease".userId = ?
        """
        cursor.execute(sql, (cursor.lastrowid,))
        row = cursor.fetchone()
        newDiseaseExhibited["diseaseId"] = row["diseaseId"]
        newDiseaseExhibited["userId"] = row["userId"]
        newDiseaseExhibited["positiveDate"] = row["positiveDate"]
    except Error as e:
        print(f"Error opening the database {e}")
        abort(500)
    finally:
        if conn:
            conn.close()

    return newDiseaseExhibited

@app.route("/contact/<int:recieverId>/diseasedContact", methods = ["POST"])
def personContacted(recieverId):
    newContact= {}
    conn = None
    try:
        jsonPostData = request.get_json()
        contactDate = jsonPostData["contactDate"]
        initiatorId = jsonPostData["initiatorId"]

        conn = sqlite3.connect("contactTracer.db")
        conn.row_factory = sqlite3.Row
        sql = """
        INSERT INTO "COMES IN CONTACT WITH"(contactDate, initiatorId, recieverId) VALUES (?,?,?)
        """
        cursor = conn.cursor()
        cursor.execute(sql, (contactDate,initiatorId,recieverId))
        conn.commit()
        sql = """
        SELECT "Comes in Contact with".contactId, "Comes in Contact with".contactDate, "Comes in Contact with".initiatorId, "Comes in Contact with".recieverId
        FROM "Comes in Contact with"
        WHERE "Comes in Contact with".contactId = ?
        """
        cursor.execute(sql, (cursor.lastrowid,))
        row = cursor.fetchone()
        newContact["contactId"] = row["contactId"]
        newContact["contactDate"] = row["contactDate"]
        newContact["initiatorId"] = row["initiatorId"]
        newContact["recieverId"] = row["recieverId"]
    except Error as e:
        print(f"Error opening the database {e}")
        abort(500)
    finally:
        if conn:
            conn.close()

    return newContact

@app.route("/positiveTest/symptoms")
def getPositiveTests():
    positiveTests = {"firstName": [], "lastName": [], "positiveDiseases": [], "symptoms": []}
    conn = None
    try:
        conn = sqlite3.connect("contactTracer.db")
        conn.row_factory  = sqlite3.Row
        sql = """
        SELECT Person.firstName, Person.lastName, Disease.diseaseName, Symptom.symptomName
        FROM Person, "Person Tested Positive for Disease", "Person has Symptom", Symptom, Disease
        WHERE Person.userId =  "Person Tested Positive for Disease".userId
        AND Person.userId = "Person has Symptom".userId
        AND Symptom.symptomId = "Person has Symptom".symptomId
        AND Disease.diseaseId = "Person Tested Positive for Disease".diseaseId
        ORDER BY Person.lastName, Person.firstName
        """
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        
        if(len(rows) ==  0):
            abort(404)
        else:
            for row in rows:
                positiveTests["firstName"].append(row["firstName"])
                positiveTests["lastName"].append(row["lastName"])
                positiveTests["positiveDiseases"].append(row["diseaseName"])
                positiveTests["symptoms"].append(row["symptomName"])

    except Error as e:
        print(f"Error opening the database {e}")
        abort(500)
    finally:
        if conn:
            conn.close()

    return positiveTests


