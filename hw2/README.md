Name: Hang Zhao   ID: 112524698

## 1: JDBC

```
db2sampl
db2start
db2 connect to sample
java -cp <classpath> SalaryStdDev <databasename> <tablename> <username> <password>
```

URL : 127.0.0.1
Default port is 50000. 

The command I used when I run this program on my MAC: 

```
db2sampl
db2start
db2 connect to sample
java -cp .:lib/db2java.jar:lib/db2jcc4.jar:lib/db2jcc_license_cu.jar SalaryStdDev SAMPLE EMPLOYEE db2inst1 hang
```

## 2: SQL PL Stored Procedure

The command I used when I run this program on my MAC: 

```
db2sampl
db2start
db2 connect to sample
db2 -td@ -f stddev.sql
```