import cx_Oracle
import codecs

def insertInto(table_name, input_list):
    ip = 'localhost'
    port = '1521'
    SID = 'xe'
    dsn_tns = cx_Oracle.makedsn(ip, port, SID)
    db = cx_Oracle.connect('SYS', '1025', dsn_tns, cx_Oracle.SYSDBA)

    insert_stmt = 'INSERT INTO "' + str(table_name) + '" VALUES (' + "'" + str(input_list[0]) + "',utl_raw.cast_to_raw('" + str(input_list[1]) + "'))"
    cursor = db.cursor()
    cursor.execute(insert_stmt)
    db.commit()
    if cursor:
        cursor.close()
    if db:
        db.close()


bin_data = open('test.py', 'rb').read()
print(bin_data)
hex_data = codecs.encode(bin_data, "hex_codec")
print(hex_data)
hex_data = hex_data.decode()
print(hex_data)
#hex_data = hex_data.encode()
#print(hex_data)
#hex_data = codecs.decode(hex_data, "hex_codec")
#print(hex_data)
job_id = 'job6'
input_list = [job_id, hex_data]
print(input_list)
insertInto("_DEPLOYED_JOBS", input_list)


