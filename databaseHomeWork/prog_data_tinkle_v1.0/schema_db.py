#-----------------------------------------------
# schema_db.py
# author: Jingyu Han   hjymail@163.com
# modified by:Shuting Guo shutingnjupt@gmail.com
#-----------------------------------------------
# to process the schema data, which is stored in all.sch
# all.sch are divied into three parts,namely metaHead, tableNameHead and body
# metaHead|tableNameHead|body
#-------------------------------------------


import ctypes
import struct
import head_db # it is main memory structure for the table schema





#the following is metaHead structure,which is 12 bytes
"""
isStored    # whether there is data in the all.sch
tableNum    # how many tables
offset      # where the free area begins for body.
"""
META_HEAD_SIZE=12                                           #the First part in the schema file


#the following is the structure of tableNameHead
"""
tablename|numofFeilds|beginOffsetInBody|....|tablename|numofFeilds|beginOffsetInBody|
10 bytes |4 bytes    |4 bytes
"""
MAX_TABLE_NAME_LEN=10                                       # the maximum length of table name
MAX_TABLE_NUM=100                                           # the maximum number of tables in the all.sch
TABLE_NAME_ENTRY_LEN=MAX_TABLE_NAME_LEN+4+4                 # the length of one table name entry
TABLE_NAME_HEAD_SIZE=MAX_TABLE_NUM*TABLE_NAME_ENTRY_LEN     # the SECOND part in the schema file



# the following is for body, which stores the field names of each table
MAX_FIELD_LEN=10                                            #the maximum size of one field name, we does not consider types and its length
MAX_NUM_OF_FIELD_PER_TABLE=5                                # the maximum number of feilds in one table
FIELD_ENTRY_SIZE_PER_TABLE=MAX_FIELD_LEN*MAX_NUM_OF_FIELD_PER_TABLE
MAX_FIELD_SECTION_SIZE=FIELD_ENTRY_SIZE_PER_TABLE*MAX_TABLE_NUM #the THIRD part in the schema file



BODY_BEGIN_INDEX=META_HEAD_SIZE+TABLE_NAME_HEAD_SIZE            # Intitially, where the field names are stored
def fillTableName(tableName): # it should be 10 bytes
    if len(tableName.strip())<MAX_TABLE_NAME_LEN:
        tableName=' '*(MAX_TABLE_NAME_LEN-len(tableName.strip()))+tableName.strip()
        return tableName
class Schema(object):
	'''
	Schema class
	'''

	fileName = 'all.sch'  # denoting the schema file name
	count = 0  # there should be only one object in the program

	@staticmethod
	def how_many():  # give the count of instances
		return count

	def viewTableNames(self):  # to list all the table names in the all.sch

		print 'viewtablenames begin to execute'
		# to be inserted here
		for i in self.headObj.tableNames:
			print 'Table name is     ', i[0]
		print 'execute Done!'

	def viewTableStructure(self, table_name):
		#print 'viewTableStructure begins to execute'
		tmp=[]
		for i in range(len(self.headObj.tableNames)):
			if self.headObj.tableNames[i][0] == table_name:
				tmp = [j.strip() for j in self.headObj.tableFields[i]]
				#print '|'.join(tmp)
		return tmp


	# ------------------------------------------------
	# constructor of the class
	# ------------------------------------------------
	def __init__(self):
		#print '__init__ of Schema'

		#print 'schema fileName is ' + Schema.fileName
		self.fileObj = open(Schema.fileName, 'rb+')  # in binary format

		# read all data from schema file
		bufLen = META_HEAD_SIZE + TABLE_NAME_HEAD_SIZE + MAX_FIELD_SECTION_SIZE  # the length of metahead, table name entries and feildName sections
		buf = ctypes.create_string_buffer(bufLen)
		buf = self.fileObj.read(bufLen)
		#print buf
		buf.strip()
		if len(buf) == 0:  # for the first time, there is nothing in the schema file
			self.body_begin_index = BODY_BEGIN_INDEX
			buf = struct.pack('!?ii', False, 0, self.body_begin_index)  # is_stored, tablenum,offset

			self.fileObj.seek(0)
			self.fileObj.write(buf)
			self.fileObj.flush()

			# the following is to create a main memory structure for the schema
			nameList = []
			fieldsList = []
			self.headObj = head_db.Header(nameList, fieldsList,False, 0, self.body_begin_index)

			#print 'metaHead of schema has been written to all.sch and the Header ojbect created'

		else:  # there is something in the schema file


			#print "there is something  in the all.sch"
			#? : bool i :int
			isStored, tempTableNum, tempOffset = struct.unpack_from('!?ii', buf, 0)   #link:https://docs.python.org/2/library/struct.html

			#print "tableNum from file is ", tempTableNum
			#print "isStored from file is ", isStored
			#print "offset of body from file is ", tempOffset
			Schema.body_begin_index = tempOffset
			nameList = []
			fieldsList = []

			if isStored == False:  # only the meta head exists, but there is no table information
				self.headObj = head_db.Header(nameList, fieldsList, False, 0, BODY_BEGIN_INDEX)
				#print "there is no table in the file"

			else:  # there is information of some table

				#print "there is at least one table in the file "
				# the following is to fetch the tableNameHead from the buffer
				for i in range(tempTableNum):
					# fetch the table name in tableNameHead
					tempName, = struct.unpack_from('!10s', buf,
					                               META_HEAD_SIZE + i * TABLE_NAME_ENTRY_LEN)  # Note: 'i' means no memory alignment
					#print "tablename is ", tempName

					# fetch the number of fields in the table in tableNameHead
					tempNum, = struct.unpack_from('!i', buf, META_HEAD_SIZE + i * TABLE_NAME_ENTRY_LEN + 10)
					#print 'number of fields of table ', tempName, ' is ', tempNum

					# fetch the offset where field names are stored in the body
					tempPos, = struct.unpack_from('!i', buf,
					                              META_HEAD_SIZE + i * TABLE_NAME_ENTRY_LEN + 10 + struct.calcsize('i'))
					#print "tempPos in body is ", tempPos

					tempNameMix = (tempName.strip(), tempNum, tempPos)
					nameList.append(tempNameMix)  # It is a triple

					# the following is to fetch field names from body section. note there is no types. It is a naive version
					if tempNum > 0:

						fields = []
						for j in range(tempNum):
							tempFieldName, = struct.unpack_from('!10s', buf, tempPos + j * 10)
							#print 'field name is ', tempFieldName
							fields.append(tempFieldName)

						fieldTuple = tuple(fields)
						fieldsList.append(fieldTuple)
				# the main memory structure for schema is constructed

				self.headObj = head_db.Header(nameList, fieldsList, True, tempTableNum, tempOffset)

	# ----------------------------
	# destructor of the class
	# ----------------------------
	def __del__(self):  # write the metahead information in head object to file

		#print "__del__ of class Schema begins to execute"

		buf = ctypes.create_string_buffer(12)

		struct.pack_into('!?ii', buf, 0, self.headObj.isStored, self.headObj.lenOfTableNum, self.headObj.offsetOfBody)
		self.fileObj.seek(0)
		self.fileObj.write(buf)
		self.fileObj.flush()
		self.fileObj.close()

	# --------------------------
	# Author: Shuting Guo shutingnjupt@gmail.com
	# delete all the contents in the schema file
	# ----------------------------------------
	def deleteAll(self):
		self.headObj.tableNames=[]
		self.headObj.tableFields=[]
		self.fileObj.seek(0)
		self.fileObj.truncate(0)
		self.headObj.isStored = False
		self.headObj.lenOfTableNum = 0
		self.headObj.offsetOfBody = self.body_begin_index
		self.fileObj.flush()
		print "all.sch file has been truncated"

	# -----------------------------
	# insert a table schema to the schema file
	# input:
	#       tablename: the table to be added
	#       fieldList: the field name list, without field type information
	# -------------------------------
	def appendTable(self, tableName, fieldList):  # it modify the tableNameHead and body of all.sch
		print "appendTable begins to execute"
		tableName.strip()

		if len(tableName) == 0 or len(tableName) > 10:
			print 'tablename is invalid'
		else:

			fieldNum = len(fieldList)

			print "the following is to write the fields to body in all.sch"
			fieldBuff = ctypes.create_string_buffer(MAX_FIELD_LEN * len(fieldList))
			beginIndex = 0
			for fieldName in fieldList:
				if len(fieldName.strip()) <= 10:
					filledFieldName = ' ' * (MAX_FIELD_LEN - len(fieldName.strip())) + fieldName.strip()
				struct.pack_into('!10s', fieldBuff, beginIndex, filledFieldName)
				beginIndex = beginIndex + MAX_FIELD_LEN

			writePos = self.headObj.offsetOfBody

			self.fileObj.seek(writePos)
			self.fileObj.write(fieldBuff)
			self.fileObj.flush()

			# self.headObj.offsetOfBody=self.headObj.offsetBody+fieldNum*MAX_FIELD_LEN

			print "the following is to write table name entry to tableNameHead in all.sch"
			filledTableName = fillTableName(tableName)

			nameBuf = struct.pack('!10sii', filledTableName, fieldNum, self.headObj.offsetOfBody)
			self.fileObj.seek(META_HEAD_SIZE + self.headObj.lenOfTableNum * TABLE_NAME_ENTRY_LEN)
			nameContent = (tableName, fieldNum, self.headObj.offsetOfBody)

			self.fileObj.write(nameBuf)
			self.fileObj.flush()

			print "to modify the header structure in main memory"
			self.headObj.isStored = True
			self.headObj.lenOfTableNum += 1
			self.headObj.offsetOfBody += fieldNum * 10
			self.headObj.tableNames.append(nameContent)
			fieldTuple = tuple(fieldList)
			self.headObj.tableFields.append(fieldTuple)

	# -------------------------------
	# Author: Shuting Guo shutingnjupt@gmail.com
	# to determine whether the table named table_name exist
	# input
	#       table_name
	# output
	#       true or false
	# -------------------------------------------------------
	def find_table(self, table_name):
		Tables = map(lambda x: x[0], self.headObj.tableNames)
		if table_name in Tables:
			return True
		else:
			return False

	def WriteBuff(self):
		bufLen = META_HEAD_SIZE + TABLE_NAME_HEAD_SIZE + MAX_FIELD_SECTION_SIZE  # the length of metahead, table name entries and feildName sections
		buf = ctypes.create_string_buffer(bufLen)
		struct.pack_into('!?ii', buf, 0, self.headObj.isStored, self.headObj.lenOfTableNum, self.headObj.offsetOfBody)
		isStored, tempTableNum, tempOffset = struct.unpack_from('!?ii', buf,0)  # link:https://docs.python.org/2/library/struct.html
		print isStored,tempTableNum,tempOffset
		for idx in range(len(self.headObj.tableNames)):
			tmp_tableName = self.headObj.tableNames[idx][0]
			if len(tmp_tableName)<10:
				tmp_tableName = ' ' * (MAX_FIELD_LEN - len(tmp_tableName.strip())) + tmp_tableName
			struct.pack_into('!10sii', buf, META_HEAD_SIZE + idx * TABLE_NAME_ENTRY_LEN, tmp_tableName,
			                 self.headObj.tableNames[idx][1],self.headObj.tableNames[idx][2])
			for idj in range(self.headObj.tableNames[idx][1]):
				struct.pack_into('!10s',buf,self.headObj.tableNames[idx][2]+idj*10,self.headObj.tableFields[idx][idj])
		self.fileObj.seek(0)
		self.fileObj.write(buf)
		self.fileObj.flush()

	# ----------------------------------------------
	# Author: Shuting Guo shutingnjupt@gmail.com
	# to delete the schema of a table from the schema file
	# input
	#       table_name: the table to be deleted
	# output
	#       True or False
	# ------------------------------------------------
	def delete_table_schema(self, table_name):
		tmpIndex=-1
		for i in range(len(self.headObj.tableNames)):
			if self.headObj.tableNames[i][0]==table_name:
				tmpIndex=i
		if tmpIndex>=0:
			self.headObj.tableNames.remove(self.headObj.tableNames[tmpIndex])
			self.headObj.tableFields.remove(self.headObj.tableFields[tmpIndex])
			self.headObj.lenOfTableNum-=1
			if len(self.headObj.tableNames):
				name_list = map(lambda x: x[0], self.headObj.tableNames)
				table_num = map(lambda x: x[1], self.headObj.tableNames)
				table_offset= map(lambda x: x[2], self.headObj.tableNames)
				table_offset[0] = BODY_BEGIN_INDEX
				for idx in range(1,len(table_offset)):
					table_offset[idx] = table_offset[idx-1] + table_num[idx-1]*10
				self.headObj.tableNames=zip(name_list,table_num,table_offset)
				self.headObj.offsetOfBody=self.headObj.tableNames[-1][2]+self.headObj.tableNames[-1][1]*10
				self.WriteBuff()

			else:
				print False
				self.headObj.offsetOfBody = BODY_BEGIN_INDEX
				self.headObj.isStored = False
			return True
		else:
			print 'Cannot find the table!'
			return False

	# ---------------------------
	# Author: Shuting Guo shutingnjupt@gmail.com
	# to return the list of all the table names
	# input
	# output
	#       table_name_list: the returned list of table names
	# --------------------------------
	def get_table_name_list(self):
		return map(lambda x:x[0],self.headObj.tableNames)
