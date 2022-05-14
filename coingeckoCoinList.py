import json
#with open('/Users/yan/Desktop/crypto research/PartTime/Smrti Lab/txTrace/4.0/response_1647408352195.json', encoding='utf-8') as f:
#line = f.readline()
#d = json.loads('/Users/yan/Desktop/crypto research/PartTime/Smrti Lab/txTrace/4.0/response_1647408352195.json')

#j = d['symbol']
#print(j)
#f.close()

#setup local files for valid tx check
top5000txt = open("top5000.txt")
top5000 = top5000txt.read()
top5000txt.close()

json_file_path = "response_1647408352195.json"
count=0
namelist=[]
with open(json_file_path, 'r') as j:
     contents = json.loads(j.read())
     for i in contents:
         namelist.append(i["symbol"].upper())
         if(i["symbol"].upper() in top5000):
             count+=1

print(namelist)
print(count)