fl_set = open("set_commands.txt", "w")
fl_get = open("get_commands.txt", "w")
fl_del = open("del_commands.txt", "w")

def set(key, val):
    return "curl -X POST -d \"{\\\"key\\\":\\\"" + key + "\\\", \\\"value\\\":\\\"" + val + "\\\"}\" http://localhost:8080/set"

def get(key):
    return "curl http://localhost:8080/get?key=" + key

def dele(key):
    return "curl -X DEL http://localhost:8080/del?key=" + key

for i in range(1000):
    fl_set.write(set(str(i), str(i)) + "\n")
    fl_get.write(get(str(i)) + "\n")
    fl_del.write(dele(str(i)) + "\n")