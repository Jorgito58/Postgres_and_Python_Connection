"""A Python Module for PostgreSQL Using psycopg2 librari

spgpy is a small python module for use the main function of 
PostgreSQL Database faster.
This is a Beta Version of spgpy.

.. _PostgreSQL: https://www.postgresql.org/
.. _Python: https://www.python.org/

@autor: Wise_George
feedback => gmail: binaryteck@gmail.com

"""
#Requeriments: psycopg2 => pip install psycopg2 (on terminal)
#Commands:
    #=> init_conection(self,host: str, database: str, user: str, password: str, port: int = 5432):
    #=> init_cursor(self):
    #=> close_conection_cursor(self):
    #=> select_one_from_table(self, table: str):
    #=> select_all_from_table(self, table: str):
    #=> delete_table(self, table: str):
    #=> delete_from_table(self, table: str, where_algoritm: str):
    #=> update_table(self, table: str, set_algoritm: str, where_algoritm: str):

import psycopg2
from datetime import datetime as dt



#Decorators
def Time_Lapse(function):
    def Wrapper(*args, **kwargs):
        initial_Time = dt.now()
        function(*args, **kwargs)
        final_Time = dt.now()
        time_lapse = (final_Time - initial_Time).microseconds
        print("RunTime: {} Microseconds\n".format(time_lapse))

    return Wrapper

def Raise_Exception(function):
    def Wrapper(*args, **kwargs):
        try:
            function(*args, **kwargs)
        except Exception as error:
            print("Exception Caught: {}".format(error))
    
    return Wrapper



class Connection(object):
    """
    Connection Class to use with psycopg2. 
    Execute Main Functions of Simple postgreSQL Faster.
    Create a Connection object also start a new connection and a new cursor.
    """

    def __init__(self,host: str, database: str, user: str, password: str, port: int = 5432) -> None:

        #Conection Settings
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.connection = self.init_conection(self.host, self.database, self.user, self.password, self.port)
        self.cursor = self.init_cursor()  
    
    
    def init_conection(self,host: str, database: str, user: str, password: str, port: int = 5432):
        """
        Initializing Connection:
            Args: (host, database, user, password, port)
            Return: connection object
        """
        try:
            connect = psycopg2.connect(
            host = host,
            database = database, 
            user = user,
            password = password,
            port = port
        )
            print("🔌Connected Succesfully to {} Database🔌".format(database))
        except Exception as error:
            print("Error Connecting to {} Database".format(database))
        return connect
    
    
    def init_cursor(self):
        """
        Initializing Cursor:
            Args: (None)
            Return: cursor object
        """
        try:
            my_cursor = self.connection.cursor()
            print("🔌Cursor Created Successfully🔌")
            return my_cursor
        except Exception as error:
            print("Error Initializing Cursor")
    
    
    
    @Raise_Exception
    def close_conection_cursor(self):
        """Close cursor and connection:
                Args: None
                Return: None
            **Always execute close_conection_cursor 
              At the end of your code**
        """
        if self.cursor is not None:
            self.cursor.close()
            print("🔌Cursor Closed Successfully🔌")
        if self.connection is not None:
            self.connection.close()
            print("🔌Connection Closed Successfully🔌\n")
    
    @Raise_Exception
    def select_one_from_table(self, table: str):
        """
        Select one from table:
        Also Select one from view:
            Args: table
            Return: __result(tuple)
        """
       
        self.cursor.execute("SELECT * FROM {}".format(table))
        __result = self.cursor.fetchone()
        print("\n==>{} table: ".format(table))
        print(__result)
        print("")
        return __result
    
    @Raise_Exception
    def select_all_from_table(self, table: str):
        """
        Select all from table:
        Also Select all from view:
            Args: table
            Return: __result(list(tuples))
        """
        
        self.cursor.execute("SELECT * FROM {}".format(table))
        result = self.cursor.fetchall()
        print("\n==>{} table: ".format(table))
        for i in range (len(result)):
            print(type(result))
            print("{}=>>{}".format(i+1, result[i]))
        print("\n")
        return result
       
    @Raise_Exception   
    def delete_table(self, table: str) -> None:
        """
        Delete Table:
            Args: table
            Return: None
        """
        self.cursor.execute("DROP TABLE {}".format(table))
        print("Table {} Deleted Successfully".format(table))
        self.connection.commit()

    @Raise_Exception
    def delete_from_table(self, table: str, where_algoritm: str):
        """
        Delete from Table:
            Args: table(str), where_algoritm(str) 
            Return: None
        """
        
        self.cursor.execute("SELECT * FROM {} WHERE {}".format(table, where_algoritm))
        __result = self.cursor.fetchall()
        if len(__result) < 1:
            print("⁉️ Error Trying to Execute:")
            print("DELETE FROM {} WHERE {}".format(table, where_algoritm)) 
            print("Instance not Found\n")
            
        else:
            self.cursor.execute("DELETE FROM {} WHERE {}".format(table, where_algoritm))
            self.connection.commit()
            print("⚠️ Instance deleted successfully from {} WHERE {}\n".format(table, where_algoritm))
        
       
    @Raise_Exception
    def update_table(self, table: str, set_algoritm: str, where_algoritm: str):
        """
        Update Table:
            Args: table(str),set_algoritm(str), where_algoritm(str) 
            Return: None
        """
        
        self.cursor.execute("SELECT * FROM {} WHERE {}".format(table, where_algoritm))
        __result = self.cursor.fetchall()
        if len(__result) < 1:
            print("⁉️ Error Trying to Execute:")
            print("DELETE FROM {} WHERE {}".format(table, where_algoritm)) 
            print("Instance not Found\n")
            
        else:
            self.cursor.execute("UPDATE {} SET {} WHERE {}".format(table,set_algoritm, where_algoritm))
            self.connection.commit()
            print("⚠️ Instance updated successfully")
            print("UPDATE {} SET {} WHERE {}\n".format(table,set_algoritm, where_algoritm))
    




