





import sqlite3
import csv

from sqlite3 import Error

def database_connection(db_location):

    my_connection  = None

    try:
        my_connection  = sqlite3.connect(db_location)
        print(sqlite3.version)
        
        return my_connection
    except Error as err:
        print(err) 


def create_table(cur):

    cur.execute("DROP TABLE IF EXISTS table1")
    
    table = """ CREATE TABLE table1(
        ID INTEGER PRIMARY KEY AUTOINCREMENT,
        LIMIT_BAL int NOT NULL,
        SEX int NOT NULL,
        EDUCATION int NOT NULL,
        MARRIAGE int NOT NULL,
        AGE int NOT NULL,
        PAY int NOT NULL,
        BILL_AMT int NOT NULL,
        PAY_AMT int NOT NULL,
        DefaultNext int NOT NULL);"""
    cur.execute(table)
    print("Table created Successfully !!")

def insert_one_element(cur):
    sql = """INSERT INTO table1 VALUES ( 1, 2500, 1, 2, 1, 24, 2, 3913, 0, 1) """
    cur.execute(sql)

    data = cur.execute(''' SELECT * FROM table1''')
    for row in data:
        print(row)


def insert_elements_from_csv(my_cursor,my_connection,fileName):
    with open(fileName) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
                sql = "INSERT INTO table1 (ID, LIMIT_BAL, SEX, EDUCATION, MARRIAGE, AGE, PAY, BILL_AMT, PAY_AMT, DefaultNext)" \
                    " VALUES({0}, '{1}', {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9})".format(row["ID"], row["LIMIT_BAL"], row["SEX"],row["EDUCATION"],
                    row["MARRIAGE"], row["AGE"], row["PAY"], row["BILL_AMT"], row["PAY_AMT"],row["DefaultNext"]
                    )
                my_cursor.execute(sql)

    print("ALL VALUES WERE INSERTED !!")
    # data = my_cursor.execute(''' SELECT * FROM table1''')
    # for row in data:
    #     print(row)
    
    # Commit your changes in the database    
    my_connection.commit()



def remove_all_negative_bill_amt(my_cursor,my_connection):
    sql = 'DELETE from table1 where BILL_AMT<0'
    my_cursor.execute(sql)
    my_connection.commit()
    sql = '''SELECT * from table1'''
    my_cursor.execute(sql)
    recs = my_cursor.fetchall()
    print(recs)
def select_ten_first_rows(my_cursor):
    sql = 'SELECT *from table1 LIMIT 10'
    my_cursor.execute(sql)
    recs = my_cursor.fetchall()
    print(recs)

def select_bill_amt_greater_than_500000(my_cursor):
    sql = 'SELECT *from table1 WHERE BILL_AMT > 500000'
    my_cursor.execute(sql)
    recs = my_cursor.fetchall()
    print(recs)


def main():
    try:

        my_connection = database_connection(r"mydatabase.db")
        my_cursor = my_connection.cursor()
        create_table(my_cursor)
        insert_elements_from_csv(my_cursor,my_connection,"CCSubset.csv")
        remove_all_negative_bill_amt(my_cursor,my_connection)
        select_ten_first_rows(my_cursor)
        select_bill_amt_greater_than_500000(my_cursor)
    finally:
       
        my_connection.close()
        print("done !")

    


if __name__ == "__main__":
    main()