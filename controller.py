from flask import Flask,request,jsonify
import mysql_client
import json
app=Flask(__name__)

# post method
@app.route('/app/employee',methods=["POST"])
def insertEmployeeInTable():
    if request.method=="POST":
        data=request.get_json()
        # print(data)
        results=mysql_client.insertEmployee(data)
    return results 

# get method
@app.route('/employee/<int:empNo>')
def getEmployeeById(empNo):
    employes=mysql_client.getEmployeeById(empNo)
    return employes

@app.route('/app/employee')
def getEmployeeWithPaging():
    page=int(request.args.get('page'))
    pageSize=int(request.args.get('pageSize'))
    name=str(request.args.get('name'))
    if name:
        employes=mysql_client.getEmployeeByName(page,pageSize,name)
        print(employes)
    else:
        employes=mysql_client.getEmployee(page,pageSize)
        print(employes)
    return employes   #error occcured

@app.route('/employee/<int:empNo>/salary')
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