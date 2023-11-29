fl = open("commands.txt", "w")

def set(key, val):
    return "curl -X POST -d \"{\\\"key\\\":\\\"" + key + "\\\", \\\"value\\\":\\\"" + val + "\\\"}\" http://localhost:8080/set"

for i in range(1000):
    fl.write(set(str(i), str(i)) + "\n")