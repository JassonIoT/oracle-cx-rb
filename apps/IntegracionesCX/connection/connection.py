import pymssql
import IntegracionesCX.connection.constants as c


class Connection:
    def __init__(self) -> None:
        pass

    def connect(self,name_db):
        conn = pymssql.connect(server=c.DB_SERVER, user=c.DB_USER, password=c.DB_PASSWORD, database=name_db)
        # cur = conn.cursor()
        return conn

    def get_version(self):
        ver = pymssql.get_pymssql.get_dbversion()
        return ver
    
    def test(self):
        #return "testing db OK"
        conn = pymssql.connect(server=c.DB_SERVER, user=c.DB_USER, password=c.DB_PASSWORD, database=c.DB_NAME)
        #print('en test')
        cur = conn.cursor()
        print('abre cursor')
        cur.execute('SELECT @@VERSION')

        for row in cur:
            ver = str(row)

        conn.close()
        return str(ver)
