import pymysql
connector=pymysql.connect(
    user="root",
    host="localhost",
    password="root",
    port=3306)
print(connector,"connected successfully✌")
