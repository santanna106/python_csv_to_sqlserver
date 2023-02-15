import camelot
import glob
import pandas as pd
import pyodbc
import unidecode 

NOME_TABELA = ' '
cabecalho = []

def pdf_to_csv():
    tables = camelot.read_pdf('NM_FILE', pages='1,2,3,4,5,6,7,8,9')
    tables.export('NM_EXPORT_CSV', f='csv')

def create_connection(auto_commit=False):
    return  pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=SERVER_NAME;'
                      'Database=DATABASE_NAME;'
                      'Trusted_Connection=yes;',autocommit=auto_commit)

def create_columns_table():
    conn = create_connection(True)
    for arquivo in glob.glob("*.csv"):
        df = pd.read_csv(arquivo)
        
        ddl_tabela = " IF OBJECT_ID (N'" + NOME_TABELA + "', N'U') IS NOT NULL DROP TABLE dbo."+NOME_TABELA+"  CREATE TABLE dbo." + NOME_TABELA + " ( "
       
        for col in df.columns:
            nome_coluna = unidecode.unidecode(col)
            nome_coluna = nome_coluna.replace(" ",'')
            cabecalho.append(nome_coluna)
            ddl_tabela += nome_coluna + " nvarchar(255),"
            
        break
    ddl_tabela = ddl_tabela[0:-1] + " ) "   
    cursor = conn.cursor()
    cursor.execute(ddl_tabela)


def csv_to_sqlserver():
    for arquivo in glob.glob("*.csv"):
        df = pd.read_csv(arquivo)
        conn = create_connection(False)
        cursor = conn.cursor()
        for i, j in df.iterrows():
            insert_tabela = "INSERT INTO dbo." + NOME_TABELA + " ( "
            insert_tabela += ",".join(cabecalho) + ")"
            insert_tabela += " VALUES ( "
            insert_tabela += " ?,?,?,?,?,?,?,? )"
            cursor.execute(insert_tabela,j[0],j[1],j[2],j[3],j[4],j[5],j[6],j[7])
            conn.commit()
            print(insert_tabela)
                

create_columns_table()
csv_to_sqlserver()
