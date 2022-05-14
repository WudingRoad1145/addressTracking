import csv
import os
from etherscan import Etherscan #etherscan api lib
from typing import Any
import moment
import time
from pymysql import NULL
import requests

class getTxAnalysis():
    def __init__(self,ccpKEY, ethKEY,addr,startBlock,endBlock):
        #CryptoCompare api Setup 
        self.URL_HIST_PRICE_HOUR = 'https://min-api.cryptocompare.com/data/v2/histohour?fsym={}&tsym={}&limit={}&e={}&toTs={}&api_key={}'
        self.ccpKEY = ccpKEY
        #etherscan api Setup
        #api key
        self.eth = Etherscan(ethKEY)
        self.addr = addr
        self.startBlock = startBlock
        self.endBlock = endBlock
        #setup local files for valid tx check
        top5000txt = open("top5000.txt")
        self.top5000 = top5000txt.read()
        top5000txt.close()
        self.DEXlist = []
        for line in open("DEXaddr.txt"):
            self.DEXlist.append(line[:42])

    def getTx(self):
        #using api to get adr erc20 tx from startblock to endblock
        result = self.eth.get_erc20_token_transfer_events_by_address(address=self.addr, startblock=self.startBlock, endblock=self.endBlock, sort=Any)
        return result
    
    def checkData(self,tx): # actually, realized that can directly compare timestamp, no need to transform to string...:()
        updated = 0 # updated = 1 if the last row time matches current time - 0 if not
        with open(tx["tokenSymbol"]+".csv", "r", encoding="utf-8", errors="ignore") as f:
            final_line = f.readlines()[-1] # this is a string
            final_line_parsed = final_line.split(",")
            currentTime = int(time.time())
            priceTick = moment.unix(currentTime).format('YYYYMMDDTHH')
            priceTickProcessed = priceTick[0:4]+"/"+priceTick[4:6]+"/"+priceTick[6:8]+" "+priceTick[9:11]+":00:00"
            latestTimeStamp = int(final_line_parsed[3])
            if (final_line_parsed[2] == priceTickProcessed):
                updated = 1
            f.close()
            
            #update the database if the price data is not updated to the nearest hour
            if(updated == 0):
                with open(tx["tokenSymbol"]+".csv","a+",newline="") as f: 
                    # get newest price data
                    row = 0
                    # add into file
                    f_csv = csv.writer(f)
                    for t in range(1,int((currentTime-latestTimeStamp)/7200000)+1):
                        cryptoQ = requests.get(self.URL_HIST_PRICE_HOUR.format(tx["tokenSymbol"], "USD",2000,"CCCAGG",int(tx["timeStamp"])+7200000*t,self.ccpKEY)).json()
                        #add price
                        for j in range(2000):
                            priceTick = moment.unix(cryptoQ["Data"]["Data"][j]["time"]).format('YYYYMMDDTHHmmss')
                            priceTickProcessed = priceTick[0:4]+"/"+priceTick[4:6]+"/"+priceTick[6:8]+" "+priceTick[9:11]+":"+priceTick[11:13]+":"+priceTick[13:]
                            row = [tx["tokenName"],tx["tokenSymbol"],priceTickProcessed, cryptoQ["Data"]["Data"][j]["time"],cryptoQ["Data"]["Data"][j]["open"],cryptoQ["Data"]["Data"][j]["close"]] 
                            f_csv.writerow(row)
                
    def getPrice(self,tx):
        price = 0 #initialize as 0
        if(tx["tokenSymbol"] in self.top5000): #Only process top5000 tokens
            #check if price data exists locally
            fileExists = os.path.exists(tx["tokenSymbol"]+'.csv')
            
            if(tx["tokenSymbol"]=="WETH" or tx["tokenName"]=="Wrapped Ether"):
                tokenPrice = (open('ETH.csv','r'))
                reader = csv.reader(tokenPrice)
                for row in reader: 
                    if(row[3]==tx["timeStamp"]):
                        price = float(row[4])
            if fileExists:
                #check data up to date
                self.checkData(tx)
                #print("*")
                print(tx["tokenSymbol"]+"  hit!!!!")
                tokenPrice = (open(tx["tokenSymbol"]+'.csv','r'))
                reader = csv.reader(tokenPrice)
                for row in reader: 
                    #data format:"TokenName","TokenSymbol","Time","Timestamp","Price-Open","Price-Close"
                    if((row[3] != "Timestamp") and (float(row[3])<=float(tx["timeStamp"])) and (float(tx["timeStamp"])<float(row[3])+3600)): #if fall in the hourly time interval
                        price = float(row[4]) #use open price
            #if price data isn't stored locally, fetch with api and build local database
            else:
                print("extra search used: "+tx["tokenName"])
                #cryptoQ=cryptocompare.get_historical_price_hour(i["tokenSymbol"], 'USD', limit=1, exchange='CCCAGG', toTs=int(i["timeStamp"]))
                momentPrice = requests.get(self.URL_HIST_PRICE_HOUR.format(tx["tokenSymbol"], "USD",1,"CCCAGG",int(tx["timeStamp"]),self.ccpKEY)).json()# Get Price at the moment
                if(momentPrice["Response"] != "Error" and momentPrice["Data"] != None):
                    #print(momentPrice)
                    price = momentPrice["Data"]["Data"][0]["open"]

                    #Build the database starting from first encounter to save api calls
                    headers = ["TokenName","TokenSymbol","Time","Timestamp","Price-Open","Price-Close","ContractAddress",tx["contractAddress"]]
                    rows = [] #initialize price data list
                    currentTime = int(time.time())
                    for t in range(1,int((currentTime-int(tx["timeStamp"]))/7200000)+1): # --- no longer endTime = 1643691600 2022/2/1 - endtime is now
                        cryptoQ = requests.get(self.URL_HIST_PRICE_HOUR.format(tx["tokenSymbol"], "USD",2000,"CCCAGG",int(tx["timeStamp"])+7200000*t,self.ccpKEY)).json()
                        #add price
                        for j in range(2000):
                            #print(cryptoQ["Data"]["Data"][j])
                            priceTick = moment.unix(cryptoQ["Data"]["Data"][j]["time"]).format('YYYYMMDDTHHmmss')
                            priceTickProcessed = priceTick[0:4]+"/"+priceTick[4:6]+"/"+priceTick[6:8]+" "+priceTick[9:11]+":"+priceTick[11:13]+":"+priceTick[13:]
                            row = [tx["tokenName"],tx["tokenSymbol"],priceTickProcessed, cryptoQ["Data"]["Data"][j]["time"],cryptoQ["Data"]["Data"][j]["open"],cryptoQ["Data"]["Data"][j]["close"]] 
                            rows.append(row)
                    with open(tx["tokenSymbol"]+".csv","w") as f:
                        f_csv = csv.writer(f)
                        f_csv.writerow(headers)
                        f_csv.writerows(rows)
        return price                        
    
    def txAnalysis(self, result):
        #Analyse ERC-20 tokens tx
        #initialize valid tx
        output = []
        for i in result:
            #filter only DEX transactions by checking with locak DEXaddr.txt file
            #if((i["to"] in self.DEXlist) or (i["from"] in self.DEXlist)): #interact with a DEX if either to or from is in DEXlist
            if(i["tokenSymbol"]=="YFI"):
                print(i)
                print(i["timeStamp"])    
                exist=0 #initialize the token traded as first appear
                if(i["to"]==self.addr): direction=1 #buy
                else: direction =-1 #sell
                amount = float(i["value"])/(10**int(i["tokenDecimal"])) #process raw amount with token decimal amount
                ret = 0 #initialize return
                time = moment.unix(int(i["timeStamp"])).format('YYYYMMDDTHHmmss')
                timeProcessed = time[0:4]+"/"+time[4:6]+"/"+time[6:8]+" "+time[9:11]+":"+time[11:13]+":"+time[13:]
                print(timeProcessed)
                #get price
                price = self.getPrice(i)
                print(i["timeStamp"])
                print("at: ",price)
                #sort by token
                for item in output:
                    #token entry exists
                    if(item["tokenContract"]==i["contractAddress"]):
                        #if sell calculate return
                        if(direction==-1 and item["totalAmount"] > 0):
                            ret = (price- item["totalCost"]/item["buyAmount"])*amount #return = (sellingPrice- averageCost)*sellingAmount//(sellingPrice*sellingAmount - averageCost*sellingAmount)
                        #if(price != 0 and item["totalCost"] != 0):
                            #PnL=ret/item["totalCost"]
                        print("ret: ",ret)
                        #txRecord- blockNumber,timeStamp,amount,price,return,P&L
                        direc = "sell"
                        totalCost = 0
                        newBuy = 0
                        if(direction == 1):
                            direc = "buy"
                            totalCost = amount*price     
                            newBuy = amount
                        tx = [i["blockNumber"],
                            timeProcessed, 
                            direc,
                            amount, #amount
                            price, #price
                            ret, #return
                            #PnL,#P&L, calc: return/totalCost
                            i["hash"] #tx hash
                            ]
                        if(ret > 0):
                            item["goodTrade"]+= 1
                            item["goodTradeList"].append(tx)
                            print("good trade: ", tx)
                        if(ret < 0):
                            item["badTrade"]+= 1
                            item["badTradeList"].append(tx)
                            print("bad trade: ", tx)
                        item["lastDirection"]=direc
                        item["buyAmount"]+=newBuy
                        item["totalAmount"]+=amount*direction
                        item["totalCost"]+=totalCost
                        item["return"]+=ret
                        #item["P&L"]=PnL,
                        item["txRecord"].append(tx)
                        item["lastTradePrice"]=price
                        exist=1
                #token entry doesn't exist
                if(exist==0 and i["tokenSymbol"] in self.top5000): #add new token entry
                    direc = "sell"
                    totalCost = 0
                    newBuy = 0
                    if(direction == 1):
                        direc = "buy"
                        totalCost = amount*price     
                        newBuy = amount
                    tx = [i["blockNumber"],
                        timeProcessed,
                        direc,
                        amount, #amount
                        price, #price
                        0, #return
                        #0,#P&L, calc: return/totalCostx
                        i["hash"]#tx hash
                        ]            
                    token={
                        "tokenName": i["tokenName"],
                        "tokenSymbol": i["tokenSymbol"],
                        "tokenContract": i["contractAddress"],
                        "firstTrade": timeProcessed,
                        "lastDirection": direc,
                        "buyAmount": newBuy,
                        "totalAmount":amount*direction,
                        "totalCost": totalCost, #would be negative if start with selling
                        "return":0, #initialized at 0
                        #"P&L":0, #initialized at 0, calc: return/totalCost
                        "goodTrade":0,
                        "goodTradeList":[],
                        "badTrade":0,
                        "lastTradePrice":price,
                        "badTradeList":[],
                        "txRecord":[tx]
                    }
                    if(ret>0):
                        token["goodTradeList"].append(tx)
                    output.append(token)
        return output
    
    #total stats count
    def totalStats(self,output):
        #setup
        self.total = 0
        self.totalReturn = 0
        self.totalGoodTrade = 0
        self.totalBadTrade = 0
        self.unrealizedProfit = 0
        rows = []

        for i in output:
            row = []
            self.total+=len(i["txRecord"])
            self.totalReturn += i["return"]
            self.totalGoodTrade += i["goodTrade"]
            self.totalBadTrade += i["badTrade"]
            #print(i)
            row = [i["tokenName"],i["tokenSymbol"],i["tokenContract"],i["firstTrade"],i["lastDirection"],i["totalAmount"],i["totalCost"],i["return"],i["goodTrade"],i["badTrade"],i["txRecord"],i["goodTradeList"],i["badTradeList"]]
            rows.append(row)
            if(i["totalAmount"]>0):
                tx = {
                    "tokenSymbol": i["tokenSymbol"],
                    "tokenName":i["tokenName"],
                    "contractAddress":i["tokenContract"],
                    "timeStamp":self.endBlock
                }
                self.unrealizedProfit += i["lastTradePrice"]*i["totalAmount"]
        return rows
    
    #output - require write-in data list rows   
    def output(self, rows): 
        goodTradeP = 0
        if(self.total != 0):
            goodTradeP = self.totalGoodTrade/self.total
        print("*****- Account info -*****")
        print("total token: ", self.total)
        print("total return: ", self.totalReturn)
        print("total good trades: ", self.totalGoodTrade)
        print("total bad trades: ", self.totalBadTrade)
        print("good trade%: ", goodTradeP)
        print("unrealized profit: ", self.unrealizedProfit)

        addrInfo = ["Address", "Total Tokens", "Total Return from DEX", "Good Trades", "Bad Trades", "Winning%","","Unrealized Profits"]
        addrSummary = [self.addr, self.total, self.totalReturn, self.totalGoodTrade, self.totalBadTrade, goodTradeP," ",self.unrealizedProfit]
        blank = []
        headers = ["Token Name","Token Symbol","Token Contract", "First Trade","Last Direction","Total Ammount","Total Cost","Return","Good Trades","Bad Trades","All Records","Good Trade Records","Bad Trade Records"]

        with open(self.addr+".csv","w") as f:
            f_csv = csv.writer(f)
            f_csv.writerow(addrInfo)
            f_csv.writerow(addrSummary)
            f_csv.writerow(blank)
            f_csv.writerow(headers)
            f_csv.writerows(rows)

#ccpKEY, ethKEY,addr,startBlock,endBlock            
sample = getTxAnalysis() 
#sampel input - "ccpKEY","ethKEY","addr you wanna track",10500000,14740788 - #2019/6/21-2022/5/9
result = sample.getTx()
output = sample.txAnalysis(result)
rows = sample.totalStats(output)
sample.output(rows)


