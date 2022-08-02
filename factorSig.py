#!/bin/python

import numpy as np
import mysql.connector

from mysql.connector import Error


def makeConnection():
	connection = None
	try:
		connection = mysql.connector.connect(host="localhost",user="scp",passwd="password",database="scp")
		return connection
	except Error as e:
		print(f"The error '{e}' occurred")
		quit()


#gets all of the votes with the conditions
def LoadVotes(connection):		
	cursor = connection.cursor()
	#cursor.execute("SELECT * FROM votes;")
	#cursor.execute("select votes.UserId, PageId, value from votes INNER JOIN (SELECT UserId FROM votes GROUP BY UserId HAVING count(value) > 10) AS filter ON votes.UserId = filter.UserId;")
	cursor.execute("select votes.UserId, PageId, value from votes INNER JOIN (SELECT UserId FROM votes where value = 1 GROUP BY UserId HAVING count(value) > 10) AS filter ON votes.UserId =filter.UserId INNER JOIN (SELECT UserId FROM votes where value = -1 GROUP BY UserId HAVING count(value) > 3) AS filter2 ON votes.UserId = filter2.UserId;")
	return np.array(list(map(lambda x: [int(x[0]),int(x[1]),float(x[2])] ,cursor.fetchall())))


def filterPages(votes):
	return np.unique(np.array(list(map(lambda x: x[0],votes))))


def filterUsers(votes):
	return np.unique(np.array(list(map(lambda x: x[1],votes))))


#gets page average (unused)
def getPageAverage(connection):
	cursor = connection.cursor()
	cursor.execute("SELECT votes.PageId, SUM(value) / (COUNT(value) + 2) FROM votes INNER JOIN (SELECT UserId FROM votes where value = 1 GROUP BY UserId HAVING count(value) > 20) AS filter ON votes.UserId = filter.UserId INNER JOIN (SELECT UserId FROM votes where value = -1 GROUP BY UserId HAVING count(value) > 5) AS filter2 ON votes.UserId = filter2.UserId GROUP BY votes.PageId;")
	return dict(cursor.fetchall())
	

#Gets user average (unused)
def getUserAverage(connection):
	cursor = connection.cursor()
	cursor.execute("SELECT votes.UserId, SUM(value) / (COUNT(value) + 2) FROM votes INNER JOIN (SELECT UserId FROM votes where value = 1 GROUP BY UserId HAVING count(value) > 20) AS filter ON votes.UserId = filter.UserId INNER JOIN (SELECT UserId FROM votes where value = -1 GROUP BY UserId HAVING count(value) > 5) AS filter2 ON votes.UserId = filter2.UserId GROUP BY votes.UserId;")
	return dict(cursor.fetchall())



#gets the average vote across all pages
def geTotalAverage(connection):
	cursor = connection.cursor()
	#cursor.execute("SELECT AVG(value) FROM votes;")
	cursor.execute("select avg(value) from votes INNER JOIN (SELECT UserId FROM votes where value = 1 GROUP BY UserId HAVING count(value) > 20) AS filter ON votes.UserId = filter.UserId INNER JOIN (SELECT UserId FROM votes where value = -1 GROUP BY UserId HAVING count(value) > 5) AS filter2 ON votes.UserId = filter2.UserId;")
	return float(cursor.fetchall()[0][0])


#Computes the errors for each vote
def computeErrors(votes,userLatent,pagesLatent, pageBias, userBias, bias):
	errors = np.zeros([len(votes),4])
	for i in range(len(votes)):
		vote = votes[i]
		page = vote[0]
		user = vote[1]
		predicted = bias + userBias[user] + pageBias[page] + np.dot(userLatent[user],pagesLatent[page])
		predicted = 1/(1+np.exp(-predicted)) 
		errors[i] = [page,user,vote[2]-(2*predicted-1),predicted*(1-predicted)]
	return errors



def updateUserLatent(errors,userLatent,pagesLatent,learningRate,regulerization):
	newUserLatent = userLatent.copy()
	for error in errors:
		newUserLatent[error[1]] += learningRate * error[2] * pagesLatent[error[0]] * error[3]

	for k in newUserLatent.keys():
		newUserLatent[k] -= learningRate * regulerization * (userLatent[k] + np.sum(userLatent[k]))

	return newUserLatent

def updatePagesLatent(errors,userLatent,pagesLatent,learningRate,regulerization):
	newPagesLatent = pagesLatent.copy()
	for error in errors:
		newPagesLatent[error[0]] += learningRate * error[2] * userLatent[error[1]] * error[3]

	for k in newPagesLatent.keys():
		newPagesLatent[k] -= learningRate * regulerization * (pagesLatent[k] + np.sum(pagesLatent[k]))

	return newPagesLatent

def updateUserBias(errors,userBias,learningRate,regulerization):
	newUserBias = userBias.copy()
	for error in errors:
		newUserBias[error[1]] += learningRate * error[2] * error[3]
	for k in newUserBias.keys():
		newUserBias[k] -= learningRate * regulerization * userBias[k]

	return newUserBias


def updatePagesBias(errors,pagesBias,learningRate,regulerization):
	newPagesBias = pagesBias.copy()
	for error in errors:
		newPagesBias[error[0]] += learningRate * error[2] * error[3]

	for k in newPagesBias.keys():
		newPagesBias[k] -= learningRate * regulerization * pagesBias[k]

	return newPagesBias


def updateBias(errors,bias,learningRate):
	for error in errors:
		bias += learningRate * error[2] * error[3] / 100
	return bias


#defs

learningRate = 0.002

regulerization = 1

latentDims = 15

itterations = 50

#read data

connection = makeConnection()

votes = LoadVotes(connection)

bias = -np.log(1/(geTotalAverage(connection)/2+0.5 ) -1)

users = filterUsers(votes)

pages = filterPages(votes)


#Test train split

np.random.shuffle(votes)

train = votes[:-100000]
test = votes[-100000:]

#init latents and biases

pagesBias = {x:0 for x in pages}
userBias = {x:0 for x in users}

usersLatent = {x:(np.random.rand(latentDims) * 2 - 1)/5  for x in users}
pagesLatent = {x:(np.random.rand(latentDims) * 2  -1)/5  for x in pages}



#update errors

for i in range(itterations):
	print("loop: " + str(i))
	errors = computeErrors(train,usersLatent,pagesLatent, pagesBias, userBias, bias)
	avg = 0
	for error in errors:
		avg += error[2] * error[2]

	avg /= len(train)

	print("error: " + str(avg))
	newUsersLatent = updateUserLatent(errors,usersLatent,pagesLatent,learningRate * 20 ,regulerization)
	newPagesLatent = updatePagesLatent(errors,usersLatent,pagesLatent,learningRate * 20,regulerization)
	userBias = updateUserBias(errors,userBias,learningRate,regulerization*0)
	pagesBias = updatePagesBias(errors,pagesBias,learningRate,regulerization*0)
	
	
	usersLatent = newUsersLatent
	pagesLatent = newPagesLatent

# check output

errors = computeErrors(test,usersLatent,pagesLatent, pagesBias, userBias, bias)

avg = 0

for error in errors:
	avg += error[2] * error[2]

avg /= 100000

print(avg)

#export paramiters

import json
from json import JSONEncoder


class NumpyArrayEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            if isinstance(obj[0], np.int64):
                return list(map(int,obj.tolist()))
            return obj.tolist()
	
        return JSONEncoder.default(self, obj)




outputData = json.dumps([usersLatent,pagesLatent,userBias,pagesBias,bias],cls=NumpyArrayEncoder)

with  open("scpData", "w") as outfile:
	outfile.write(outputData)

