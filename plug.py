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


def getPage(connection, pageName):
	try:
		querry = "select PageId from pages where Name = %s"
		cursor = connection.cursor(prepared = True)
		cursor.execute(querry,(pageName,))
		return float(cursor.fetchall()[0][0])
	except Error as e:
		print(f"the error '{e}' occurred")
		quit()



def getVotes(connection, pageid):
	try:
		querry = "select UserId from votes where PageId = %s"
		cursor = connection.cursor(prepared = True)
		cursor.execute(querry,(pageid,))
		return np.ndarray.flatten(np.array(cursor.fetchall()))
	except Error as e:
		print(f"the error '{e}' occurred")
		quit()



def getUserName(connection, userId):
	try:
		querry = "select DisplayName from users where UserId = %s"
		cursor = connection.cursor(prepared = True)
		cursor.execute(querry,(userId,))
		return cursor.fetchall()[0]
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

pageid = getPage(connection,sys.argv[1])


#how many differnt parts
count = int(sys.argv[2])


filteredUsers = []


#If the user wants a tag to be filterd
filteredUsers = np.array(list(map(lambda x: int(float(x)), list(userBias.keys()))))


print(pagesLatent[str(pageid)])


filteredUsers = np.setdiff1d(filteredUsers, getVotes(connection,pageid))



userSupport = []


#Compute all scores 
for user in filteredUsers:
	if not str(float(user)) in userBias:
		print("cannot find user id: " + str(user)+ " skipping")
		continue
	prob = np.dot(usersLatent[str(float(user))],pagesLatent[str(float(pageid))]) + pagesBias[str(float(pageid))] + userBias[str(float(user))] + bias
	prob = 1/(1+np.exp(-prob))
	userSupport.append([user,prob])

#Sorts the data
def pagesort(data):
	return data[1]

userSupport.sort(key=pagesort,reverse=True)

#prints the top X points
for i in range(count):
	userData = getUserName(connection, int(userSupport[i][0]))
	print(str(i+1)+": "+ userData[0]+"\tscore: "+str(userSupport[i][1]))














