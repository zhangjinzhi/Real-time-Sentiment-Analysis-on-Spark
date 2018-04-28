import socket  

def client(text):  
    address = ('127.0.0.1', 31500)  
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  
    s.connect(address)  
    b2 = bytes(text,encoding='utf8')
    s.send(b2)
 
    data = s.recv(1000) 
    s.close()
 
    return data  
  

  
#    s.close() 

text = "I like trump."
#print(client(text))
