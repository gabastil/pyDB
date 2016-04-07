#!/usr/bin/python
# -*- coding: utf-8 -*-
# 
# Name:		Database.py
# Version:	1.0.0
# Author:	Glenn Abastillas
# Date:		March 21, 2016
# 
# Purpose: Allows the user to:
# 			1.) Log into a database
#			2.) Run queries against the database
#			3.) Pull data from the database
#
# To see the script run, go to the bottom of this page.
#
# This class is directly inherited by the following classes:
#	- BackupVariants.py
# - - - - - - - - - - - - -
"""	connect to, access, and manipulate specified servers and databases within Microsoft SQL Server.

The Database class allows the user to access databases on specified servers
and run SQL script to manipulate their contents. This class is preinitial-
ized to connect to the DICEDictionary on Carol's server (i.e., W0157340). 
This class utilizes the pymssql package to perform connections, run queries,
retrieve results, and close connections.

"""
import pymssql

class Database(object):
	
	def __init__(self):
		self.username  = None
		self.password  = None
		self.sqlserver = None
		self.servers   = {"JOE":"W0176179", "JOE-OLD":"W0141227", "CAROL":"W0157340", "ME":"W01613453"}

	def close(self):
		"""	close the connection stream
		"""
		self.sqlserver.close()
		print("> Connection closed")

	def connect(self, server="CAROL", database="DICEDictionary"):
		"""	connect to a SQL server specified in this object
			@param	username: log-in username credenusactial
			@param	password: log-in password
			@param	server: server name (e.g., "JOE", "CAROL", "JOE-OLD")
			@param	database: database name (e.g., "dbo.Variants", "dbo.Concepts") 
		"""
		server = server.upper()
		self.sqlserver = pymssql.connect(server=self.servers[server], database=database)
		print("> Connection established: {0}:{1}:{2}".format(server, self.servers[server], database))

	def getCursor(self):
		"""	Get the cursor
			@return	class cursor in self.sqlserver
		"""
		return self.sqlserver.cursor()

	def runScript(self, script):
		"""	run a script against the server and get results as a list
			@param	script: SQL script
			@return	list of 
		"""
		cursor = self.sqlserver.cursor()
		cursor.execute(script)

		output = list()
		append = output.append

		for row in cursor:
			append([str(item) for item in row])

		return output

	def runMultipleInsert(self, script, listOfValues):
		"""	run a script to insert some value(s) to the database
			@param	script: insert statement for sequel
			@param	listOfValues: list of tuples with values to insert
		"""
		cursor = self.sqlserver.cursor()
		cursor.executemany(script, listOfValues)

	def switchServer(self, server, database):
		"""	switch from current server to specified server
			@param	server: server name (e.g., "JOE", "CAROL", "JOE-OLD")
			@param	database: database name
		"""
		if self.sqlserver is not None:
			print("> Switching to database {0} in server {1}".format(database.upper(), server.upper()))
			self.sqlserver.close()
			self.connect(server=self.servers[server], database=database)
		else:
			print("> No connections to any server established.")

	def addServer(self, name, server):
		"""	add a new server to the list of servers in this Database object
			@param	name: personal server name (e.g., "Glenn")
			@param	server: server name (e.g., W0163453)
		"""
		self.servers[name.upper()] = server.upper()
		print("> Added server {0} as {1} to this Database servers list".format(server.upper(), name.upper()))

	def getServer(self, name):
		"""	retrieve the server name specified by the person name
			@param	name: personal name of the server
			@return	server name
		"""
		return self.servers[name.upper()]

	def getPassword(self):
		"""	get this class's self.password
			@return	this class's password
		"""
		return self.password

	def getUsername(self):
		"""	get this class's self.username
			@return	this class's username
		"""
		return self.username

	def setPassword(self, password):
		"""	set this class's self.password
			@param	password: new password
		"""
		self.password = password

	def setUsername(self, username):
		"""	set this class's self.username
			@param	username: new username
		"""
		self.username = username

if __name__=="__main__":
	server = "CAROL"
	database = "DICEDictionary"
	script = "SELECT DISTINCT * FROM dbo.Variants WHERE notes LIKE '%03/21/2016 GA%' AND variant like '%pregnancy%';"
	db = Database()
	db.connect(server=server, database=database)
	output = db.runScript(script)
	db.close()
	print output[:2]
	frame = "{0}\t{1}\t{2}\t{3}\t{4}"
	for row in output[:2]:
		print frame.format(row[0], row[1], row[2], row[3], row[4])

	db.addServer("Glenn", "W0163453")
	print db.getServer("Glenn")