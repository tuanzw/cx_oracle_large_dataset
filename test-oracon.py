import cx_Oracle
import csv
import datetime


def append_to_outfile(filename, rows):
    with open(filename, 'a') as f_out:
        #output = csv.writer(f_out, dialect='excel')
        output = csv.writer(f_out, delimiter=',', lineterminator="\n", quoting=csv.QUOTE_NONE, escapechar='\\')
        for row in rows:
            output.writerow(row)

def get_sql_statement_from_file(filename):
    sql = ''
    with open(filename, 'r') as f_in:
        for line in f_in:
            sql += line
    return sql
    
def extract_data_to_file():
    CONN_INFO = {
        'host': 'host',
        'port': 1521,
        'user': 'user',
        'pwd': 'password',
        'service': 'sid',
    }

    CONN_STR = "{user}/{pwd}@{host}:{port}/{service}".format(**CONN_INFO)

    out_filename = r"C:\output.csv"
    sql_filename = r"C:\sql_statement.sql"
    total_record = 0
    sql_statement = get_sql_statement_from_file(sql_filename)
    print(sql_statement)
    
    with cx_Oracle.connect(CONN_STR) as connection:

        print("Database version:", connection.version)
        
        
        with connection.cursor() as cursor:
        
            cursor.arraysize = 1000
            
            begin_time = datetime.datetime.now()
            cursor.execute(sql_statement)
            print(f"sql_excecute_elapsed_time = {datetime.datetime.now() - begin_time}") 
            
            header_cols = []
            for col in cursor.description:
                header_cols.append(col[0])
                
            #append_to_outfile(out_filename, [header_cols] )
                
            while True:
                rows = cursor.fetchmany()
                rownum = len(rows)
                if rownum < 1:
                    print("No more row")
                    break
                else:
                    print(f"Currently exported {total_record}, to export the next: {rownum} records")
                    total_record += rownum
                    append_to_outfile(out_filename, rows)
    print(f"Total records exported: {total_record}")
    print(f"*******excecute_elapsed_time = {datetime.datetime.now() - begin_time}")
                    
if __name__ == "__main__":
    extract_data_to_file()

