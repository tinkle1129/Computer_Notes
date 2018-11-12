#----------------------------------------------
# main_db.py
# author: Jingyu Han   hjymail@163.com
# modified by:Shuting Guo shutingnjupt@gmail.com
#----------------------------------------------
# This is the main loop of the program
#----------------------------------------------
import struct
import sys
import ctypes


import subprocess
import head_db          # the main memory structure table schema
import schema_db        # the module to process table schema
import storage_db       # the module to process storage of table data
import mega_sfw         # for select from where clause
import query_plan_db    # for SQL clause
import lex_db           # for lex
import parser_db        # for yacc
import common_db        # the global variables, functions, constants in the program
import query_plan_db    # construct the query plan and execute it

PROMPT_STR='Input your choice  \n1:add new table \n2:delete table \n3:view tables \n4:delete all data \n5: SFW clause \n6:record operation \n. to quit):\n'
import mega_storage # the storage module  only for megatron 2000

def monitor():

	subprocess.call("clear")  # linux/mac
	# subprocess.call("cls", shell=True)  # windows
	print '#----------------------------------------------------#'
	print '#  Design and Implementation of Database Kernel      #'
	print '#     Author : Jinyu Han hjymail@163.com             #'
	print '#  Modified by : Shuting Guo shutingnjupt@gmail.com  #'
	print '#----------------------------------------------------#'

def main():
	print '#    There are two storage in this system            #'
	print '#    1: megatron2000        0: Storage               #'
	print '#----------------------------------------------------#'

	mark =1
	while(mark):
		MEGA_TAG=raw_input('please enter the select  :                     ')
		if MEGA_TAG.strip() == '1':
			MEGA_TAG = True
			mark = 0
		elif MEGA_TAG.strip() == '0':
			MEGA_TAG = False
			mark = 0

	if MEGA_TAG:
		schemaObj = schema_db.Schema()  # to create a schema object, which contains the shchema of all tables
		choice = raw_input(PROMPT_STR)
		while True:

			monitor()
			if choice == '1':
				print '#        Your Query is to add new table              #'
				tableName = raw_input('please enter your table name:')
				if schemaObj.find_table(tableName):
					print 'This table'+tableName+'already exists!'
					print 'Structure show below:'
					print schemaObj.viewTableStructure(tableName)
				else:
					numberOfFields = raw_input('please enter the number of fields:')
					if int(numberOfFields) > 0:
						nameList = []
						for i in range(int(numberOfFields)):
							aName = raw_input("please enter the field name:")
							aName = ' '*(10-len(aName))+aName
							nameList.append(aName)
						schemaObj.appendTable(tableName, nameList)  # store the table schema into all.sch
						dataObj = mega_storage.MegaStorage(tableName)
						del dataObj
					else:
						print "there is nothing to do for the number of feilds"
				print '#----------------------------------------------------#'

				choice = raw_input(PROMPT_STR)

			elif choice == '2':
				print '#        Your Query is to delete a table             #'
				print 'Table List mentioned below\n', schemaObj.get_table_name_list()
				table_name = raw_input('please input the name of the table to be deleted:')
				if schemaObj.find_table(table_name.strip()):
					if schemaObj.delete_table_schema(table_name):  # delete the schema from the schema file
						dataObj = mega_storage.MegaStorage(table_name)  # create an object for the data of table
						dataObj.delete_table_data()  # delete table content from the table file
						del dataObj
					else:
						print 'the deletion from schema file fail'
				else:
					print 'there is no table ' + table_name + ' in the schema file'
				print '#----------------------------------------------------#'

				choice = raw_input(PROMPT_STR)

			elif choice == '3':
				print '#       Your Query is to view all tables             #'
				schemaObj.headObj.showTables()  # to show the structure of all tables
				print '#----------------------------------------------------#'
				choice = raw_input(PROMPT_STR)

			elif choice == '4':
				print '#       Your Query is to delete all tables           #'
				table_name_list = schemaObj.get_table_name_list()
				for filename in table_name_list:
					dataObj = mega_storage.MegaStorage(filename)  # create an object for the data of table
					dataObj.delete_table_data()  # delete table content from the table file
				schemaObj.deleteAll()  # to delete all the schema of tables
				print '#----------------------------------------------------#'
				choice = raw_input(PROMPT_STR)

			elif choice == '5':
				sql_str = raw_input('please enter the select from where clause:')
				sql_ptr = mega_sfw.MegaSfw(schemaObj)  # create an object for the clause
				sql_ptr.process_sfw(sql_str)  # process the clause
				print '#----------------------------------------------------#'
				choice = raw_input(PROMPT_STR)

			elif choice == '6':
				mark = 1
				while(mark>0):
					monitor()
					print '#        Your Query is to record operation           #'
					if schemaObj.headObj.showTables():
						table_name = raw_input('Please select the table you want to modify above:')
						if schemaObj.find_table(table_name.strip()):
							mark =0
					else:
						mark=-1
				if mark == 0:
					dataObj = mega_storage.MegaStorage(table_name)  # create an object for the data of table
					nameList = schemaObj.viewTableStructure(table_name)
					while(True):
						recordchoice = raw_input('Input your choice  \n1:add new record \n2:delete record \n3:modify record \n4:show records:\n')
						if recordchoice == '1':     #add
							dataObj.insert_record(nameList)  # insert one record into the table, storing it in a file with tableName.dat

						elif recordchoice == '2':   #delete
							deletefield = raw_input('Please input the field you want to delete:'+'|'.join(nameList)+'\n')
							deleterecord = raw_input('Please input record you want to delete:\n')
							dataObj.del_one_record( (deletefield.strip(),deleterecord.strip()), nameList)

						elif recordchoice == '3':   #modify
							modifyfield = raw_input('Please input the field you want to delete:'+'|'.join(nameList)+'\n')
							beforerecord = raw_input('Please input record you want to modify:\n')
							afterrecord = raw_input('Please input record you want to change to :\n')
							dataObj.update_record((modifyfield.strip(),beforerecord.strip()), (modifyfield.strip(),afterrecord.strip()), nameList)

						elif recordchoice == '4':   #view
							recordList = dataObj.get_record_list()
							monitor()
							print 'Your Query is to show all record in  %s' %table_name.strip()
							print '|'.join(nameList)
							for record in recordList:
								print '|'.join(record)
						else:
							break
				print '#----------------------------------------------------#'
				choice = raw_input(PROMPT_STR)

			else:
				print 'main loop finishies'
				del schemaObj
				break

	else:
		schemaObj = schema_db.Schema()  # to create a schema object, which contains the shchema of all tables
		choice = raw_input(PROMPT_STR)

		while True:

			monitor()
			if choice == '1':
				print '#        Your Query is to add new table              #'
				tableName = raw_input('please enter your table name:')
				if schemaObj.find_table(tableName):
					print 'This table'+tableName+'already exists!'
					print 'Structure show below:'
					print schemaObj.viewTableStructure(tableName)
				else:
					dataObj = storage_db.Storage(tableName)
					#print list(dataObj.getFieldName())
					schemaObj.appendTable(tableName, list(dataObj.getFieldName()))
				print '#----------------------------------------------------#'

				choice = raw_input(PROMPT_STR)

			elif choice == '2':
				print '#        Your Query is to delete a table             #'
				print 'Table List mentioned below\n', schemaObj.get_table_name_list()
				table_name = raw_input('please input the name of the table to be deleted:')
				if schemaObj.find_table(table_name.strip()):
					if schemaObj.delete_table_schema(table_name):  # delete the schema from the schema file
						dataObj = storage_db.Storage(table_name)  # create an object for the data of table
						dataObj.delete_all_data()  # delete table content from the table file
						del dataObj
					else:
						print 'the deletion from schema file fail'
				else:
					print 'there is no table ' + table_name + ' in the schema file'
				print '#----------------------------------------------------#'

				choice = raw_input(PROMPT_STR)

			elif choice == '3':
				print '#       Your Query is to view all tables             #'
				schemaObj.headObj.showTables()  # to show the structure of all tables
				print '#----------------------------------------------------#'
				choice = raw_input(PROMPT_STR)

			elif choice == '4':
				print '#       Your Query is to delete all tables           #'
				table_name_list = schemaObj.get_table_name_list()
				for filename in table_name_list:
					dataObj = storage_db.Storage(filename)  # create an object for the data of table
					dataObj.delete_all_data()  # delete table content from the table file
				schemaObj.deleteAll()  # to delete all the schema of tables
				print '#----------------------------------------------------#'
				choice = raw_input(PROMPT_STR)

			elif choice == '5':
				monitor()
				print '#        Your Query is to SQL QUERY                  #'
				sql_str = raw_input('please enter the select from where clause:')
				lex_db.set_lex_handle()  # to set the global_lexer in common_db.py
				parser_db.set_handle()  # to set the global_parser in common_db.py

				try:
					common_db.global_syn_tree = common_db.global_parser.parse(sql_str.strip(),
					                                                          lexer=common_db.global_lexer)  # construct the global_syn_tree
					reload(query_plan_db)
					query_plan_db.construct_logical_tree()
					query_plan_db.execute_logical_tree()
				except:
					print 'WRONG SQL INPUT!'
				print '#----------------------------------------------------#'
				choice = raw_input(PROMPT_STR)

			elif choice == '6':
				mark = 1
				while(mark>0):
					monitor()
					print '#        Your Query is to record operation           #'
					if schemaObj.headObj.showTables():
						table_name = raw_input('Please select the table you want to modify above:')
						if schemaObj.find_table(table_name.strip()):
							mark =0
					else:
						mark=-1
				if mark == 0:
					dataObj = storage_db.Storage(table_name)  # create an object for the data of table
					nameList = schemaObj.viewTableStructure(table_name)
					while(True):
						#monitor()
						recordchoice = raw_input('Input your choice  \n1:add new record \n2:delete record \n3:show records:\n. to quit\n')
						if recordchoice == '1':     #add
							input_record=[]
							for record in nameList:
								tmp=raw_input(record+':\n')
								input_record.append(tmp)
							dataObj.insert_record(input_record)  # insert one record into the table, storing it in a file with tableName.dat

						elif recordchoice == '2':   #delete
							deletefield = raw_input('Please input the field you want to delete:'+'|'.join(nameList)+'\n')
							deleterecord = raw_input('Please input record you want to delete:\n')
							dataObj.delete_table_data((deleterecord.strip(),deletefield.strip()))

						elif recordchoice == '3':   #view
							monitor()
							dataObj.show_table_data()
						else:
							break
					del dataObj

				print '#----------------------------------------------------#'
				choice = raw_input(PROMPT_STR)

			else:
				print 'main loop finishies'
				del schemaObj
				break
if __name__ == '__main__':
	monitor()
	main()
