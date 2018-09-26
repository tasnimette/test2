# -*- coding: utf-8 -*-
"""
Created on Thu Sep 13 15:55:53 2018

@author: EYA
"""

"""
import logging

log = logging.getLogger()
log.setLevel('DEBUG')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)
"""
import pandas as pd
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

KEYSPACE = "laloatatfqna"

def BulkCassandra(dfe,session,valt):   
    ls=list(dfe.iloc[[0]])
    dfe.columns=ls
    dfe.reindex(ls)
    rows, columns = dfe.shape
    print(str(columns))
    val="Values ("
    x=0
    field=""
    typ=''
    dfe = dfe.applymap(str)
    for i in range(x,columns):
        val=val+"?,"
        x=x+1
        field=field+"field"+str(x)+","
        typ=typ+"field"+str(x)+" varchar"+","
        
        print(str(x))
    
    field=field[0:len(field)-1]
    typ=typ[0:len(typ)-1]
    val=val[0:len(val)-1]+")"  
    print(field)
    print(typ)
    print(val)

    q="""CREATE TABLE """+ """df"""+str(valt)+ """("""+typ+""", PRIMARY KEY (field1))"""
    print(q)
    
    session.execute(q)
    
    q2=""" INSERT INTO df"""+ str(valt)+ """("""+field+""") """+val
    print(q2)
    prepared = session.prepare(q2)
    
    for i, row in dfe.iterrows():
        try:
            print(tuple(row))
            session.execute(prepared,tuple(row))
        except Exception:
            print(" ")
    future = session.execute_async("SELECT * FROM df"+str(valt))
    

    try:
        rows = future.result()
    except Exception:
        print("pb")

    for row in rows:
        print('\t'.join(row))


   


   

    
if __name__ == "__main__":
    dfe=pd.read_excel("C:\\Users\\EYA\\Desktop\\DQ\\Juillet.xlsx","Périmètre reprise",skiprows=[0])
    
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()

    session = cluster.connect()
    session.execute("""
        CREATE KEYSPACE %s
        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2' }
        """ % KEYSPACE)

    #log.info("creating keyspace...")
    session.set_keyspace(KEYSPACE)
    BulkCassandra(dfe,session,1)
    dfe1=pd.read_excel("C:\\Users\\EYA\\Desktop\\DQ\\Juillet.xlsx","Périmètre reprise",skiprows=[0])
    BulkCassandra(dfe1,session,2)
    session.execute("DROP KEYSPACE " + KEYSPACE)
