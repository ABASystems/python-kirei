# Use "tsql -H <host> -L" to get list of ports.
#

import pymssql
import argparse


parser = argparse.ArgumentParser()
parser.add_argument('--in-host')
parser.add_argument('--in-port')
parser.add_argument('--in-user')
parser.add_argument('--in-pass')
parser.add_argument('--in-dbname')
args = parser.parse_args()


in_conn = pymssql.connect(
    args.in_host, args.in_user, args.in_pass, args.in_dbname,
    port=args.in_port
)


def get_columns(conn, table):
    sql = """
SELECT 
    c.name 'Column Name',
    t.Name 'Data type',
    c.max_length 'Max Length',
    c.precision ,
    c.scale ,
    c.is_nullable,
    ISNULL(i.is_primary_key, 0) 'Primary Key'
FROM    
    sys.columns c
INNER JOIN 
    sys.types t ON c.user_type_id = t.user_type_id
LEFT OUTER JOIN 
    sys.index_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id
LEFT OUTER JOIN 
    sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
WHERE
    c.object_id = OBJECT_ID('{}')
"""
    cursor = conn.cursor()
    cursor.execute(sql.format(table))
    return list(cursor)


def get_type(columns):
    if columns[1].lower() == 'datetime':
        return 'timestamp'
    elif columns[1].lower() == 'varbinary':
        return 'bytea'
    elif columns[1].lower() == 'uniqueidentifier':
        return 'varchar'
    elif columns[1].lower() == 'nvarchar':
        return 'varchar'
    elif columns[1].lower() == 'image':
        return 'varchar'
    else:
        return columns[1]


def escape(val):
    return val.replace('\'', '\'\'').replace('"', '""')


def encode(type, value):
    if type.lower() == 'varbinary':
        return 'NULL'
    elif type.lower() == 'uniqueidentifier':
        return 'NULL'
    elif type.lower() == 'nvarchar':
        return 'NULL'
    elif type.lower() == 'image':
        return 'NULL'
        # return 'E\'%s\''%value.replace('\\x', '').replace('b\'', '\\x').replace('@', '')
    else:
        return '\'%s\''%escape(value)


def encode_csv(type, value):
    if type.lower() == 'varbinary':
        return ''
    elif type.lower() == 'uniqueidentifier':
        return ''
    elif type.lower() == 'nvarchar':
        return ''
    elif type.lower() == 'image':
        return ''
        # return 'E\'%s\''%value.replace('\\x', '').replace('b\'', '\\x').replace('@', '')
    else:
        return '%s'%escape(value)


def create_table(conn, table, columns):
    table = table.strip()
    sql = 'DROP TABLE IF EXISTS {}'.format(table)
    cursor = conn.cursor()
    cursor.execute(sql)
    sql = 'CREATE TABLE IF NOT EXISTS {} ('.format(table)
    col_sql = []
    for col in columns:
        col_sql += ['{} {}'.format(col[0].strip().replace(' ', '_'), get_type(col))]
    sql += '{});'.format(', '.join(col_sql))
    cursor.execute(sql)


def copy_data_csv(in_conn, table, columns):
    with open('{}.csv'.format(table.lower()), 'w') as out_file:
        in_cur = in_conn.cursor()
        in_cur.execute('SELECT * FROM {}'.format(table))
        out_file.write(','.join(['"{}"'.format(c[0].lower()) for c in columns]) + '\n')
        for row in in_cur.fetchall():
            out_file.write(','.join(['"{}"'.format(encode_csv(c[1], str(v)) if v not in ['', None] else '') for v, c in zip(row, columns)]) + '\n')


in_cursor = in_conn.cursor()
in_cursor.execute('SELECT TABLE_NAME FROM {}.INFORMATION_SCHEMA.Tables'.format(args.in_dbname))
for row in in_cursor.fetchall():
    print(row)
    columns = get_columns(in_conn, row[0])
    copy_data_csv(in_conn, row[0], columns)
