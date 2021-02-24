import sqlite3

class Helper:
    def __dict_factory(cursor, row):
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d

    def __init__(self, file):
        self.con = conn = sqlite3.connect(file)
        self.con.row_factory = Helper.__dict_factory

    def __check_database_schema(self, base, table_name, columns):
        bcolumns = [i for i in base.execute(f'PRAGMA table_info({table_name})')]

        if len(bcolumns)==0: # table doesn't exist
            base.execute(f'CREATE TABLE {table_name} (__id integer primary key autoincrement)')

        bcolumns_names = [c['name'] for c in bcolumns]
        for cname in columns: #check columns
            if cname not in bcolumns_names:
                base.execute(f'ALTER TABLE {table_name} ADD COLUMN {cname.lower()} TEXT')

    def __save(self, table_name, row):
        base = self.con.cursor()
        columns = [c for c in row.keys()]
        values = [row[c] for c in row.keys()]
        
        self.__check_database_schema(base, table_name, columns)

        if '__id' not in columns:
            scolumns = ','.join(columns)
            svalues = (',?'*len(columns))[1:]
            s = f'INSERT INTO {table_name} ({scolumns}) VALUES ({svalues});'
            base.execute(s, values)
            new_id = int(base.execute('select last_insert_rowid() as id;').fetchone()['id'])
        else:
            primeiro = True
            s = f'UPDATE {table_name} SET '
            columns_reversed = columns[::-1]
            values_reversed = values[::-1]
            for column in [c for c in columns_reversed if c != '__id']:
                if not primeiro:
                    s += ','
                s += f'{column} = ? '
                primeiro = False
            s += f'WHERE __id = ?'
            base.execute(s, values_reversed)
            new_id = values[columns.index('__id')]
        base.close()

        return new_id

    def save(self, **data):
        new_ids = []
        base = self.con.cursor()
        for table in data:
            rows = data[table]
            
            if not isinstance(rows,list):
                temp = []
                temp.append(rows)
                rows=temp

            for row in rows:
                new_ids.append(self.__save(table, row))

        self.con.commit()
        return self.search(pessoa=dict(__id=new_ids))

    def search(self, **data):
        queries_result = []
        base = self.con.cursor()
        for table in data:
            query = f'SELECT * FROM {table}'
            parameters = data[table]
            if parameters is not None:
                query += ' WHERE 0 = 0'
                for cname in parameters:
                    parameter = parameters[cname]
                    if isinstance(parameter, list):
                        qlist = ','.join([str(v) for v in parameters[cname]])
                        query += f' AND {cname} in ({qlist})'
                    elif isinstance(parameter, str):
                        query += f' AND {cname} = \'{parameter}\''
                    elif isinstance(parameter, int) or isinstance(parameter, float):
                        if parameter.is_integer():
                            parameter = int(parameter)
                        query += f' AND {cname} = {parameter}'

            query_result = base.execute(query).fetchall()
            queries_result.append(query_result)
        base.close()
        return queries_result
    
    def __del__(self):
        self.con.close()  

if __name__ == '__main__':
    h = Helper('teste.db')
    # h.save(pessoa=dict(nome='Helder Gurgel'))
    # h.save(pessoa=[dict(nome='Helder Gurgel'),dict(nome='Ana Carolina', idade=9)])
    # print(h.save(pessoa=[dict(nome='Helder Gurgel')]))
    print(h.search(pessoa=dict(idade=18.0)))
    del h