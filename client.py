import  os, sys, pymysql
from xmlrpc.client import ServerProxy, Binary

class Client:
    db_name = "filesystem"
    db_user = "root"
    db_pw = "pw"

    remote_host = "127.0.0.1"
    servers = {}
    num_servers = 2
    if len(sys.argv)>1:
        num_servers=int(sys.argv[1])
    firstPortNum=11001        
    for i in range(num_servers):
        servers[i+1]=("127.0.0.1", firstPortNum+i)

    commands = {}
    command_info = [
    "clean                             reset and initialise the client and server machines",
    "push local_file remote_file       write a file to the remote file system", 
    "pull remote_file local_file       read a file from remote file system",
    "ls                                list all files",
    "rm remote_file                    remove a file",
    "help                              return info about commands", 
    "clear                             clear the console",
    "exit                              quit the application"]
    
    def __init__(self):
        Client.commands = { "clean": Client.clean,"push": Client.push, "pull": Client.pull, 
        "ls": Client.ls, "rm": Client.rm, "help": Client.help}

    def create_server_proxy(server_id):
        host = Client.servers[server_id][0]
        port = Client.servers[server_id][1]
        return ServerProxy("http://" + host + ":" + str(port) + "/")
    
    def create_connection(clean=False):
        return pymysql.connect(host=Client.remote_host, user=Client.db_user, 
        password=Client.db_pw, db=(Client.db_name if not clean else None), cursorclass=pymysql.cursors.DictCursor)
    
    def clean(self):
        connection = Client.create_connection(clean=True)
        with connection.cursor() as cursor:
            sql = "DROP DATABASE IF EXISTS " + Client.db_name
            cursor.execute(sql)
            sql = "CREATE DATABASE " + Client.db_name
            cursor.execute(sql)
        connection.commit()

        connection = Client.create_connection()
        with connection.cursor() as cursor:
            sql = "CREATE TABLE `file_info` (`file_id` INT PRIMARY KEY, `file_name` VARCHAR(50) NOT NULL," \
                  "`file_size` INT NOT NULL, `server_id` INT NOT NULL, UNIQUE (`file_name`))"
            cursor.execute(sql, ())

            for server_id in Client.servers.keys():
                proxy = Client.create_server_proxy(server_id)
                proxy.clean(Client.db_name)
        connection.commit()
        print("Clean up and initialisation completed")

    def push(self, local_path, file_name):
        connection = Client.create_connection()
        with connection.cursor() as cursor:
            sql = "SELECT `server_id`, SUM(`file_size`) AS `total_file_size` FROM `file_info` "\
                  "GROUP BY `server_id` ORDER BY `total_file_size` LIMIT 1"
            cursor.execute(sql, ())
            result = cursor.fetchone()
            if not result: #file_info is empty
                server_id = 1
                file_id = 1
            else:     
                server_id = result["server_id"]                
                sql = "SELECT DISTINCT(`server_id`)  FROM `file_info` ORDER BY server_id ASC"
                cursor.execute(sql, ())
                result = cursor.fetchall()
                missing_server_id=1
                for row in result:
                    if row["server_id"]!=missing_server_id:
                        server_id=missing_server_id
                        break
                    missing_server_id+=1
                if missing_server_id<=Client.num_servers:
                    server_id=missing_server_id
                sql = "SELECT MAX(`file_id`) AS `file_id` FROM `file_info`"
                cursor.execute(sql, ())
                result = cursor.fetchone()
                file_id = result["file_id"] + 1
        try:
            file_size = os.path.getsize(local_path)
        except:
            print(f"error: local file '{local_path}' does not exist")
            return

        proxy = Client.create_server_proxy(server_id)
        with open(local_path, "rb") as handle:
            data = Binary(handle.read())
            connection = Client.create_connection()
            cursor=connection.cursor()
            try:
                sql = "INSERT INTO file_info (`file_id`, `file_name`,  `server_id`, `file_size`" \
                      ") VALUES (%s, %s, %s, %s)"
                cursor.execute(sql, (file_id, file_name,  server_id, file_size,))
                connection.commit()
            except pymysql.err.IntegrityError:
                print(f"error: remote file '{file_name}' already exists")
                return
            proxy.push(data, file_id, file_name, file_size, Client.db_name)
            print("File added")

    def pull(self, file_name, local_path):
        connection = Client.create_connection()
        with connection.cursor() as cursor:
            sql = "SELECT `file_id`, `server_id` FROM `file_info` WHERE `file_name` = %s"
            cursor.execute(sql, (file_name,))
            if cursor.rowcount==0:
                print(f"error: remote file '{file_name}' does not exist")
                return
            result = cursor.fetchone()
            file_id = result["file_id"]
            server_id = result["server_id"]
        proxy = Client.create_server_proxy(server_id)
        with open(local_path, "wb") as handle:
            handle.write(proxy.pull(file_id, Client.db_name).data)
        print("File retrieved")

    def ls(self):
        file_names = []
        connection = Client.create_connection()
        with connection.cursor() as cursor:
            sql = "SELECT `file_name` FROM `file_info`"
            cursor.execute(sql)
            row = cursor.fetchone()
            while row:
                file_names.append(row["file_name"])
                row = cursor.fetchone()
        if len(file_names) > 0:
            print("Files: ")
        for item in file_names:
            print(item)

    def rm(self, file_name):
        connection = Client.create_connection()
        with connection.cursor() as cursor:
            sql = "SELECT `file_id`, `server_id` FROM `file_info` WHERE `file_name` = %s "
            cursor.execute(sql, (file_name))
            row = cursor.fetchone()
            if row:
                file_id = row["file_id"]
                proxy = Client.create_server_proxy(row['server_id'])
                proxy.remove(file_id, Client.db_name)
                sql = "DELETE FROM `file_info` WHERE `file_id` = %s"
                cursor.execute(sql, (file_id,)  )
                connection.commit()
            else:
                print(f"error: remote file '{file_name}' does not exist")
                return
        print("File removed")

    def help(self):
        print()
        for line in Client.command_info:
            print(line)

    def clear(self):
        os.system('clear')

    def command_handler(self, command):
        command_list = command.split(" ")
        func = Client.commands[command_list.pop(0)]
        command_list.insert(0, self)
        func(*command_list)
    
def main():
    client = Client()
    os.system("clear")
    while True:
        try:
            command = input("\n$ ").strip()
            if command == "exit":
                raise KeyboardInterrupt
            elif command == "clear":
                os.system("clear")
            else:
                client.command_handler(command)
        except KeyError:
            print(f"error: invalid command '{command}'\n")
        except ConnectionRefusedError:
            print("error: connection refused (some servers may not be running)\n")
        except pymysql.err.OperationalError as e:
            print(e)
            print("error: could not connect to database\n")
        except KeyboardInterrupt:
            print("\nExiting...\n")
            return
        except TypeError:
            print("error: invalid arguments passed")
        except Exception as e:
            print("error: something went wrong\n")
            print(e)
if __name__ == "__main__":  
    main()