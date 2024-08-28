import pymysql
pymysql.install_as_MySQLdb()
from sqlalchemy import (BigInteger, Boolean, Column, Date, Float, Integer, String,TIMESTAMP,
                        DateTime, create_engine, exc, Numeric, func,ForeignKey,Table,MetaData)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker,relationship,mapped_column
from sqlalchemy import func, desc

Base = declarative_base()

meta=MetaData()
class Employees(Base):
    __tablename__ = 'employees'
    emp_no= mapped_column(Integer, primary_key=True)
    birth_date = mapped_column(Date)
    first_name = mapped_column(String)
    last_name = mapped_column(String)  
    gender= mapped_column(String)
    hire_date = mapped_column(Date) 
    
class Salaries(Base):
    __tablename__ ='salaries'
    emp_no= mapped_column(Integer,ForeignKey("employees.emp_no"), primary_key=True)
    salary = mapped_column(String)
    from_date = mapped_column(Date,primary_key=True)
    to_date = mapped_column(Date)  
    
    
class Titles(Base):
    __tablename__ ='titles'
    emp_no= mapped_column(Integer,ForeignKey("employees.emp_no"),primary_key=True)
    title = mapped_column(String,primary_key=True)
    from_date = mapped_column(Date,primary_key=True)
    to_date = mapped_column(Date)  


class Dept_manager(Base):
    __tablename__ ='dept_manager'
     
    emp_no= mapped_column(Integer,ForeignKey("employees.emp_no"),primary_key=True)
    dept_no= mapped_column(String,ForeignKey("departments.dept_no"),primary_key=True)
    from_date = mapped_column(Date)
    to_date = mapped_column(Date)  


class Dept_emp(Base):
    __tablename__ ='dept_emp'
     
    emp_no= mapped_column(Integer,ForeignKey("employees.emp_no"),primary_key=True)
    dept_no= mapped_column(String,ForeignKey("departments.dept_no"),primary_key=True)
    from_date = mapped_column(Date)
    to_date = mapped_column(Date) 

class Departments(Base):
    __tablename__ ='departments'
     
    dept_no= mapped_column(String,primary_key=True)
    dept_name=mapped_column(String)