
import time

# a = 0
# while True:
#      a +=1
#      time.sleep(5)
#      print(a)
#      if a == 3:
#          break
check_status = 'available'
status = 'available'

num = 0
for i in range(0,10,1):
    num +=1
    time.sleep(6)
    print(num)

while (num != 10):
    
    print('Not match!')
    time.sleep(2)
    if  num == 10:
        break
else:
    print('match!')
