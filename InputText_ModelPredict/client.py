import socket  

def client(text):  
    address = ('127.0.0.1', 31500)  
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  
    s.connect(address)  
 
    s.send(text)
 
    data = s.recv(1000) 
    s.close()  
    return data  
  


text = "I like the trump."
client(text)
