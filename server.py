import sys, pymysql
from xmlrpc.server import SimpleXMLRPCServer

class Server:
    host = "127.0.0.1"
    port = int(sys.argv[1])
    db_user = "root"
    db_pw = "pw"
    id = port%1000

    def get_id(self):
        return Server.id

    def create_connection(db):
        return pymysql.connect(host=Server.host, user=Server.db_user,password=Server.db_pw,db=db,cursorclass=pymysql.cursors.DictCursor)

    def clean(self, db): 
        connection = Server.create_connection(db)
        with connection.cursor() as cursor:
            sql = "CREATE TABLE server%s_files (`id` INT PRIMARY KEY," \
                  " `name` VARCHAR(50) NOT NULL, `data` BLOB, `size` INT NOT NULL)"
            cursor.execute(sql, (Server.id, ))
        connection.commit()
        return "Clean up completed"

    def push(self, file_data, file_id, file_name, file_size, db):
        connection = Server.create_connection(db)
        with connection.cursor() as cursor:
            sql = "INSERT INTO server%s_files (id, name, data, size) VALUES (%s, %s, %s, %s)"
            data = file_data.data
            cursor.execute(sql, (Server.id, file_id, file_name, data, file_size))
        connection.commit()
        return "File added"

    def pull(self, file_id, db):
        connection = Server.create_connection(db)
        with connection.cursor() as cursor:
            sql = "SELECT `data` FROM `server%s_files` WHERE `id` = %s"
            cursor.execute(sql, (Server.id, file_id))
            result = cursor.fetchone()
            data = result["data"]
        return data

    def remove(self, file_id, db):
        connection = Server.create_connection(db)
        with connection.cursor() as cursor:
            sql = "DELETE FROM `server%s_files` WHERE `id` = %s"
            cursor.execute(sql, (Server.id, file_id))
        connection.commit()
        return "File removed"

try:
    server = SimpleXMLRPCServer((Server.host, Server.port))
    server.register_introspection_functions()
    server.register_instance(Server())
    host, port = server.server_address
    print("\nServer" + str(Server.id) + " is listening at " + str(host)+ ":" + str(port))
    server.serve_forever()
except OSError:
    print(f"\nerror: port #{Server.port} is not free")