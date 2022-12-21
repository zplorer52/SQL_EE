# mysqlclient==2.1.1
import mysql.connector as mysql_con
# cx_Oracle==7.2
import cx_Oracle
import json
import time

## Author: Adhir Dutta
# Email: <dutta_a@mibholding.com>
# Application: Latest Single database Row exchange among tables in
# MySQL<==>Oracle
# MySQL<==>MySQL
# Oracle<==>Oracle

# pip install

# TO-DO
# Read json configuration file 
#

# Read from Dictionary
# template
cred = [
    {
        "active": True,
        "comm": [
            {
                "dbtype": "oracle",
                "name": "AREANAME",
                "ip": "172.16.16.122",
                "user": "USER",
                "password": "PASSWORD",
                "sid": "YOUR-SERVICE-NAME",
                "port": 1521,
                "send": ["USER.S_TABLE1"],
                "receive": ["USER.R_CPY_TABLE2"]
            },
            {
                "dbtype": "oracle",
                "name": "AREANAME2",
                "ip": "172.16.16.248",
                "user": "USER2",
                "password": "PASSWORD2",
                "sid": "YOUR-SERVICE-NAME2",
                "port": 1521,
                "send": ["USER2.S_TABLE2"],
                "receive": ["USER2.S_CPY_TABLE1"]
            },
        ],
    },    
    {
        "active": True,
        "comm": [
            {
                "dbtype": "oracle",
                "name": "AREANAME",
                "ip": "172.16.16.122",
                "user": "USER",
                "password": "PASSWORD",
                "sid": "YOUR-SERVICE-NAME",
                "port": 1521,
                "send": ["USER.S_TABLE3"],
                "receive": ["USER.R_CPY_TABLE4"]
            },
            {
                "dbtype": "mysql",
                "name": "AREANAME3",
                "ip": "172.16.16.148",
                "user": "USER3",
                "password": "PASSWORD3",
                "database": "YOUR-DATABASE-NAME",
                "port": 3306,
                "send": ["USER3.S_TABLE4"],
                "receive": ["USER3.R_CPY_TABLE3"]
            },
        ],
    },
]

# Return Dictionary with
# Target Database cred.
# Update SQL list.
def getdata(db_src_cred, db_target_cred):
    # Primary DB connection credentials, Dict    
    db1 = db_src_cred
    # Secondary DBs' list of receive table names, List
    db2_receive = db_target_cred["receive"]
    db2_type = db_target_cred["dbtype"]
    # List of UPDATE SQL strings
    sql_store = None
    # Blank Database Connection Object for Primary Database
    conn = None
    try:        
        if(db1["dbtype"]=="oracle"):
            # print("Connecting Oracle")
            dsn = cx_Oracle.makedsn(db1['ip'], db1['port'], service_name=db1['sid'])
            # print(dsn)
            conn = cx_Oracle.connect(user=db1['user'], password=db1['password'], dsn=dsn, encoding="UTF-8")
            # DEBUG MSG
            # print(db1["name"], "Successfully connected to", db1["dbtype"], "with :", cx_Oracle.clientversion())

        elif(db1["dbtype"] == "mysql"):
            # print("Connecting MySQL")
            conn = mysql_con.connect(user=db1['user'], password=db1['password'], host=db1['ip'], database=db1['database'])
            # DEBUG MSG
            # print(f"Successfully connected to {db1['name'].upper()} Mysql database {db1['database']}")
        else:
            # DEBUG MSG
            print("No database connection")
            return False

        if conn:            
            sql_store = []
            for idx, table_name in enumerate(db1["send"]):
                # print(idx, table_name)                
                # Collect the Latest Row               
                q_string = f"SELECT * FROM {table_name} WHERE RINDEX=(SELECT MAX(RINDEX) FROM {table_name})"                                
                u_string = ""
                with conn.cursor() as cursor:
                    cursor.execute(q_string)                    
                    data = cursor.fetchone()
                    # print(type(data))
                    # print("row data:", data)
                    if data:
                        u_string = f'UPDATE {db2_receive[idx]} SET '                        
                        # Table column names
                        colnames = [column_header[0] for column_header in cursor.description]
                        # print(colnames, len(colnames))
                        # Mapping Column name with Data                        
                        for count, (col_name, val) in enumerate(zip(colnames, data)):
                            # print("count:",count, "colname:",col_name, "value:",val)
                            # Discard these column name/values if found
                            col_name = col_name.upper()
                            if col_name not in ('AUSER', 'ADATE'):                                                                
                                # convert LDATE as Oracle Date String
                                # print(col_name)
                                if db2_type=="oracle" and col_name in ('IDATE', 'LDATE'): 
                                    # print("for Date conversion")                                                              
                                    val = "TO_DATE('{a}', 'yyyy-mm-dd hh24:mi:ss')".format(a=val)
                                elif db2_type=="mysql" and col_name in ('IDATE', 'LDATE'):                                    
                                    val = "STR_TO_DATE('{a}', '%Y-%m-%d %H:%i:%s')".format(a=val) 
                                elif col_name == 'RINDEX':
                                    val = f"{val}"
                                else:
                                    if val is None or val == 'NULL':
                                        val = 0.0
                                    # else:
                                    #     val = round(val,3)
                            
                                u_string += f"{col_name}={val}"
                                if(count<len(colnames)-1):
                                    u_string +=","
                            
                # Update SQL for each table
                sql_store.append(u_string)
    except Exception as e:
        # DEBUG MSG
        print(e)        

    # close connections:
    if conn:
        try:
            conn.close()
            conn = None
            # print("connection is closed")
        except Exception as e:
            print(e)
    
    block_dict ={}
    block_dict["target"]= db_target_cred
    block_dict["usql"]= sql_store
    
    return block_dict

  
# Execute Update SQL list
def setdata(target=None):
    r_flag =False    
    # Empty List of SQL strings
    if not isinstance(target, dict):
        # Nothing to update
        # print("Information are empty or invalid..")
        # print("*"*40)
        return r_flag
    if not target['usql'] or len(target['usql'])<=0:
        # print(target)
        # {'target': {'dbtype': 'mysql', 'name': 'pm12', 'ip': '<ip>', 'user': 'user', 'password': 'pass', 'database': 'AAAAA', 'port': 3306, 'send': ['v_pm_pulp2'], 'receive': []}, 'usql': []}
        # print(f"No data to be transferred to {target['target']['name']}.")
        return r_flag

    
    u_sqls = target['usql']
    # Database credentials
    db2 = target['target']
    # Database type 
    dbtype = db2['dbtype']
    # Connection Object
    conn = None
    try:
        if dbtype == 'mysql':
            conn = mysql_con.connect(user=db2['user'], password=db2['password'], host=db2['ip'], database=db2['database'])
        elif dbtype == 'oracle':
            dsn = cx_Oracle.makedsn(db2['ip'], db2['port'], service_name=db2['sid'])
            # print(dsn)
            conn = cx_Oracle.connect(user=db2['user'], password=db2['password'], dsn=dsn, encoding="UTF-8")
        else:
            print("Nothing to do here!")   
            return r_flag
        
        if conn:
            for u_sql in u_sqls:
                with conn.cursor() as cursor:
                    cursor.prepare(u_sql) 
                    cursor.execute(u_sql) 
                    conn.commit()
                    # Updated successfully
                    # print("Updated successfully")
                    r_flag = True
            # Close the connection
            conn.close()
            # print("connection is closed.")
    except Exception as e:
        print(e)

    return r_flag

  
# Single Block Data Exchange
def datacom(group=None):
    # Check Blocks' Active status
    if group["active"]:
        block = group["comm"]
        name1, name2 = block[0]["name"], block[1]["name"]
        # Get Update SQL List with Remote Database credential as Dict
        l_sql1 = getdata(block[0], block[1])
        # Get Update SQL List with Remote Database credential as Dict
        l_sql2 = getdata(block[1], block[0])
        
        # Execute/Update Database Table
        setdata(target=l_sql1)
        # Execute/Update Database Table
        setdata(target=l_sql2)
        print(f"Data are exchanged in between {name1.upper()} <==> {name2.upper()} area.")

        
# Schedule it for Every minute        
def main():
    # Automate it    
    runflag = True
    while runflag:
        try:
            t = time.localtime()
            hr, min, sec = t.tm_hour, t.tm_min, t.tm_sec
            if hr in range(23) and min in range(60) and sec==0:
                print("Timestamp: {}".format(time.strftime('%m/%d/%Y %H:%M:%S', t)))
                print("**"*30)
                # DEBUG
                # datacom(group=cred[4])
                # cred[0]
                for area_block in cred:
                    datacom(group=area_block)
                    # print(area_block)

                # time.sleep(60)
        except KeyboardInterrupt as e:
            print("Closing application")
            runflag = False
        except Exception as e:
            runflag = False


# Application Entry point
if __name__ == "__main__":
    main()

