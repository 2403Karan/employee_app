from flask import Flask,request,jsonify
import mysql_client
import json
app=Flask(__name__)

# post method
@app.route('/employee',methods=["POST"])
def insertEmployeeInTable():
    if request.method=="POST":
        data=request.get_json()
        results=mysql_client.insertEmployee(data)
    return results 

# get method
@app.route('/employee/<int:empNo>')      # getting records of employee
def getEmployeeById(empNo):
    date=request.args.get('date')
    employes=mysql_client.getEmployeeById(empNo,date)
    return employes

@app.route('/pagination/employee')       # getting records in paging form
def getEmployeeWithPaging():
    page=int(request.args.get('page'))
    pageSize=int(request.args.get('pageSize'))
    name=str(request.args.get('name'))
    employes=[]
    if name=="None":
        employes=mysql_client.getEmployee(page,pageSize)
        print(employes)
    else:
        employes=mysql_client.getEmployeeByName(page,pageSize,name)
        print(employes)    
    return employes   

@app.route('/employee/<int:empNo>/salary')   # getting salary record of employee
def getSalaryOfEmployee(empNo):
    date=request.args.get('date')
    salary=mysql_client.getSalary(empNo,date)
    return salary

# put method
@app.route('/employee/<int:empNo>',methods=['PUT'])
def updateEmployeeData(empNo):
    data=request.get_json()
    return mysql_client.updateRecordsOfEmployee(empNo,data)

# delete method
@app.route('/employee/<int:empNo>',methods=["DELETE"])
def deleteEmployeesData(empNo):
    return mysql_client.deleteEmployeeData(empNo)
       
    
    
if __name__=="__main__":
    app.run()