import sqlite3

class Helper:
    def __init__(self, file):
        self.con = conn = sqlite3.connect(file)

    def __check_database_schema(self, base, table_name, columns):
        bcolumns = [i for i in base.execute(f'PRAGMA table_info({table_name})')]

        if len(bcolumns)==0: # table doesn't exist
            base.execute(f'CREATE TABLE {table_name} (__id integer primary key autoincrement)')

        bcolumns_names = [c[1] for c in bcolumns]
        for cname in columns: #check columns
            if cname not in bcolumns_names:
                base.execute(f'ALTER TABLE {table_name} ADD COLUMN {cname.lower()} TEXT')

    def __save(self, table_name, row):
        base = self.con.cursor()
        columns = [c for c in row.keys()]
        values = [row[c] for c in row.keys()]
        
        self.__check_database_schema(base, table_name, columns)

        scolumns = ','.join(columns)
        svalues = (',?'*len(columns))[1:]
        s = f'INSERT INTO {table_name} ({scolumns}) VALUES ({svalues})'
        base.execute(s, values)
        base.close()

    def save(self, **data):
        base = self.con.cursor()
        for table in data:
            rows = data[table]
            
            if not isinstance(rows,list):
                temp = []
                temp.append(rows)
                rows=temp

            for row in rows:
                self.__save(table, row)

        self.con.commit()
    
    def __del__(self):
        self.con.close()  

if __name__ == '__main__':
    h = Helper('teste.db')
    # h.save(pessoa=dict(nome='Helder Gurgel'))
    h.save(pessoa=[dict(nome='Helder Gurgel'),dict(nome='Ana Carolina', idade=9)])
    del h