import json
from pymongo import MongoClient


def regions():
    with open("Data\\data.json", "r") as f:
        data = json.load(f)
    return list(data.keys())


class DatabaseManager:
    def __init__(self, db_uri):
        self.client = MongoClient(db_uri)
        self.db = self.client.get_default_database()

    def create_table(self):
        pass

    def add_data(self, table_name, name=None, surname=None, age=None, student_id=None, advisor_id=None):
        if table_name != "student_advisor":
            self.db[table_name].insert_one({"name": name, "surname": surname, "age": age})
        else:
            self.db[table_name].insert_one({"student_id": student_id, "advisor_id": advisor_id})

    def get_existing_relations(self):
        return list(self.db["student_advisor"].find({}, {"_id": 0}))

    def delete_row(self, table_name, row_id):
        if table_name == "advisors":
            self.db[table_name].delete_one({"advisor_id": row_id})
        else:
            self.db[table_name].delete_one({"student_id": row_id})

    def load_data(self, table_name):
        return list(self.db[table_name].find({}, {"_id": 0}))

    def search(self, table_name, name=None, surname=None, age=None, student_id=None, advisor_id=None):
        query = {}
        if student_id:
            query["student_id"] = student_id
        if advisor_id:
            query["advisor_id"] = advisor_id
        if name:
            query["name"] = name
        if surname:
            query["surname"] = surname
        if age:
            query["age"] = age

        return list(self.db[table_name].find(query, {"_id": 0}))

    def update(self, table_name, name, surname, age, id):
        if table_name == "students":
            self.db[table_name].update_one({"student_id": id}, {"$set": {"name": name, "surname": surname, "age": age}})
        elif table_name == "advisors":
            self.db[table_name].update_one({"advisor_id": id}, {"$set": {"name": name, "surname": surname, "age": age}})

    def check_bd(self):
        return self.db["student_advisor"].count_documents({}) == 0

    def list_advisors_with_students_count(self, order_by):
        pipeline = [
            {"$lookup": {
                "from": "student_advisor",
                "localField": "advisor_id",
                "foreignField": "advisor_id",
                "as": "students"
            }},
            {"$addFields": {
                "student_count": {"$size": "$students"}
            }},
            {"$sort": {"student_count": 1 if order_by == "ASC" else -1}}
        ]
        return list(self.db["advisors"].aggregate(pipeline))

    def list_students_with_advisors_count(self, order_by):
        pipeline = [
            {"$lookup": {
                "from": "student_advisor",
                "localField": "student_id",
                "foreignField": "student_id",
                "as": "advisors"
            }},
            {"$addFields": {
                "advisor_count": {"$size": "$advisors"}
            }},
            {"$sort": {"advisor_count": 1 if order_by == "ASC" else -1}}
        ]
        return list(self.db["students"].aggregate(pipeline))
