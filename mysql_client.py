import pymysql
pymysql.install_as_MySQLdb()
from sqlalchemy import (BigInteger, Column, Date, Float, Integer, String,TIMESTAMP,
                        DateTime, create_engine, exc, Numeric,delete)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import func, desc, and_,between
from sqlalchemy.dialects.mysql import insert
from sqlalchemy import delete,select,join,text
import json
from model import Employees,Salaries,Departments,Dept_manager,Dept_emp,Titles
import os
import datetime
from flask import Flask,request,jsonify
 
connectionString = 'mysql://%s:%s@%s/%s' % ('pymsql','pymsql123','127.0.0.1:3306','employees')
Base = declarative_base()
engine = create_engine(connectionString, isolation_level="READ UNCOMMITTED", pool_recycle=3600)
Base.metadata.bind = engine
Base.metadata.create_all(engine)
DBsession = sessionmaker(bind=engine)
session = DBsession()


def convertDate(dt):
    return dt.strftime("%Y-%m-%d")

def recordInJson(records):
    obj_arr=[]
    for row in records:
        print(row)
        employee= {
            'empNo': row.emp_no,
            'birthDate': convertDate(row.birth_date),
            'firstName':row.first_name,
            'lastName': row.last_name,
            'gender':row.gender,
            'hireDate':convertDate(row.hire_date),
            'salary':row.salary,
            'department':row.dept_no,
            'departmentName':row.dept_name,
            'title':row.title
            
        }
        obj_arr.append(employee)
    json_data=json.dumps(obj_arr,indent=4)
    return json_data

def salary(records):
    myObj=[]
    for row in records:
        print(row)
        salary={
            'salary':row.salary,
            'fromDate':convertDate(row.from_date),
            'toDate':convertDate(row.to_date)
        }
        myObj.append(salary)
    json_data=json.dumps(myObj,indent=4)
    return json_data
        
    


# INSERT EMPLOYEES(POST METHOD)
def insertEmployee(dataToBeInserted):
    try:
        results=session.execute(Employees.__table__.insert(),dataToBeInserted)
        print(dataToBeInserted)
        print(results.inserted_primary_key)
        session.commit()
        inserted_id=results.inserted_primary_key[0] if results.inserted_primary_key else None
        print(inserted_id)
        # if QUERY_LOGGING:
        #     print('Inserted entry: %s' % str(dataToBeInserted))
        return  { "emp_no": inserted_id}
    except Exception as e:
        print("Could not insert into Employee:%s, Error: %s" % (
            str(dataToBeInserted), str(e)))
        raise
    

# GET INFORMATION OF EMPLOYEES(GET METHOD)
def getEmployeeById(employeeId):
    # query=session.query(Employees).filter(Employees.emp_no == employeeId).all()
    # recordInJson(query)
    
    join_stmt=join(Employees,Salaries,Employees.emp_no==Salaries.emp_no )\
            .join(Dept_emp,Employees.emp_no==Dept_emp.emp_no)\
            .join(Titles,Employees.emp_no==Titles.emp_no)\
            .join(Departments,Dept_emp.dept_no==Departments.dept_no)
    stmt=select(Employees,Salaries,Dept_emp,Titles,Departments).select_from(join_stmt)\
        .where(Employees.emp_no == employeeId)
    print(stmt)
    with engine.connect() as conn:
        records=conn.execute(stmt).fetchall()
        return recordInJson(records)    

def getSalary(empNo,date):
    print(date)
    if date is not None:
        stmt=select(Salaries).where(and_((Salaries.emp_no==empNo),between(date,Salaries.from_date,Salaries.to_date)))
    else:
        stmt=select(Salaries).where(Salaries.emp_no==empNo).order_by(desc(Salaries.from_date))
    print(stmt)
    with engine.connect() as conn:
        records=conn.execute(stmt).fetchall()
        return salary(records)
    

    

def getEmployee(page,page_size):
    records=session.query(Employees).offset((page - 1) * page_size).limit(page_size).all()
    return recordInJson(records)    

def getEmployeeByName(page,page_size,name):
    stmt1=select(Employees).where(func.concat(Employees.first_name, Employees.last_name).like(f'%{name}%')).offset((page - 1) * page_size).limit(page_size)
    with engine.connect() as con:
        records=con.execute(stmt1).all()
    return recordInJson(records)

# def getEmployeeFullInformation():
#     join_stmt=Employees.join(Titles,Employees.emp_no==Titles.emp_no)\
#                        .join(Salaries,Employees.emp_no==Salaries.emp_no)\
#                        .join(Dept_manager,Employees.emp_no==Dept_manager.emp_no)\
#                        .join(Departments,Dept_manager.dept_no==Departments.dept_no)
#     stmt=select(Employees,Salaries,Titles,Departments,Dept_manager).select_from(join_stmt)
#     with engine.connect() as con:
#         results=con.execute(stmt)
#         for row in results:
#             print(f"employees: {dict(row._mapping[Employees])}")
#             print(f"salaries: {dict(row._mapping[Salaries])}")
#             print(f"title: {dict(row._mapping[Titles])}")
#             print(f"department: {dict(row._mapping[Departments])}")
#             print(f"dept_manager: {dict(row._mapping[Dept_manager])}")

def updateRecordsOfEmployee(empNo,data):
    try:
        jsonObj={}
        if 'birthDate' in data:
            jsonObj["birth_date"] = data['birthDate']   # you're assigning the value from the data['birthDate'] dictionary key to the jsonObj["birth_date"] key. 
        if 'firstName' in data:
            jsonObj['first_name']=data['firstName'],
        if 'lastName' in data:
            jsonObj['last_name']= data['lastName'],
        if 'gender' in data:
            jsonObj['gender']=data['gender'],
        if 'hireDate' in data:
            jsonObj['hire_date']=data['hireDate']   
        stmt = Employees.__table__.update().values(jsonObj).where(Employees.emp_no==empNo)
        session.execute(stmt)
        session.commit()
        return  { "result": "succesfully updated"}
    except Exception as e:
        print("Could not update into Employees table")
        raise
    

def deleteEmployeeData(empNo):
    try:
        stmt = delete(Employees).where(Employees.emp_no == empNo)
        print(stmt)
        session.execute(stmt)
        session.commit()
        return {"result":"succesfully deleted"}
    except Exception as e:
        print("Could not deleteEmployeesData")
        raise
   

def commitSession():
    session.commit()
