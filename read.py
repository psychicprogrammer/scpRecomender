import json 
import mysql.connector
import sys
import numpy as np
from mysql.connector import Error

def makeConnection():
	connection = None
	try:
		connection = mysql.connector.connect(host="localhost",user="scp",passwd="password",database="scp")
		return connection
	except Error as e:
		print(f"the error '{e}' occurred")
		quit()


def getUser(connection, userName):
	try:
		querry = "select UserId from users where DisplayName = %s"
		cursor = connection.cursor(prepared = True)
		cursor.execute(querry,(userName,))
		result = cursor.fetchall()
		print(result)
		if len(result) == 0:
			return 0.0
		return float(result[0][0])
	except Error as e:
		print(f"the error '{e}' occurred")
		quit()



def getVotes(connection, userid):
	try:
		querry = "select PageId from votes where UserId = %s"
		cursor = connection.cursor(prepared = True)
		cursor.execute(querry,(userid,))
		return np.ndarray.flatten(np.array(cursor.fetchall()))
	except Error as e:
		print(f"the error '{e}' occurred")
		quit()

def getTags(connection, tagName):
	try:
		querry = "select PageId from tags where tag = %s"
		cursor = connection.cursor(prepared = True)
		cursor.execute(querry,(tagName,))
		return np.ndarray.flatten(np.array(cursor.fetchall()))
	except Error as e:
		print(f"the error '{e}' occurred")
		quit()


def getPageName(connection, pageId):
	try:
		querry = "select Name, Title from pages where PageId = %s"
		cursor = connection.cursor(prepared = True)
		cursor.execute(querry,(pageId,))
		return cursor.fetchall()[0]
	except Error as e:
		print(f"the error '{e}' occurred")
		quit()

def getPageVotes(connection, pageId):
	try:
		querry = "select sum(value) from votes where PageId = %s"
		cursor = connection.cursor(prepared = True)
		cursor.execute(querry,(int(pageId),))
		return float(cursor.fetchall()[0][0])
	except Error as e:
		print(f"the error '{e}' occurred")
		quit()





#read the latent varibles from file
with open("scpData","r") as inFile:
	rawdata = inFile.readlines()
	data = json.loads(rawdata[0])
	usersLatent = data[1]
	pagesLatent = data[0]
	userBias = data[3]
	pagesBias = data[2]
	bias = data[4]



connection = makeConnection()

#gets the userID of the given username

userid = getUser(connection,sys.argv[1])


#how many differnt parts
count = int(sys.argv[2])


filteredPages = []


#If the user wants a tag to be filterd
if len(sys.argv) == 4:
	tags = sys.argv[3].split(" ")
	for i in range(len(tags)):
		if i == 0:
			filteredPages = getTags(connection,tags[i])
		else:
			filteredPages = np.union1d(filteredPages,getTags(connection,tags[i]))

else: #Else jsut do all of the pages
	filteredPages = np.array(list(map(lambda x: int(float(x)), list(pagesBias.keys()))))

if str(userid) in usersLatent:
	print(usersLatent[str(userid)])
else:
	usersLatent[str(userid)] = np.zeros(15)
	userBias[str(userid)] = 0

if userid != 0.0:
	filteredPages = np.setdiff1d(filteredPages, getVotes(connection,userid))



userSupport = []


#Compute all scores 
for page in filteredPages:
	if not str(float(page)) in pagesBias:
		continue
	#voteCount = getPageVotes(connection, page)
	prob = np.dot(usersLatent[str(userid)],pagesLatent[str(float(page))]) + pagesBias[str(float(page))] + userBias[str(userid)] + bias# - np.log(voteCount)/5
	prob = 1/(1+np.exp(-prob))
	userSupport.append([page,prob])

#Sorts the data
def pagesort(data):
	return data[1]

userSupport.sort(key=pagesort,reverse=True)

#prints the top X points
for i in range(count):
	pageData = getPageName(connection, int(userSupport[i][0]))
	
	print(str(i+1)+": "+ pageData[1]+"\tscore: "+str(userSupport[i][1])+"\tlink: https://scp-wiki.wikidot.com/"+pageData[0] )














