#---------------------------------
# head_db.py
# author: Jingyu Han    hjymail@163.com
#--------------------------------------
# the main memory structure of table schema
# 
#------------------------------------
import struct





class Header(object): 
    #------------------------
    # constructor of the class
    # input
    #   nameList    : table name list and each element in it is a triple (table_name,num_of_fields, offset_in_body)
    #   fieldList   : field list for all tables and each element is also a tuple containing all the field names
    #   inLen       : number of tables
    #   off         : where the free space begins in body of the schema file
    #---------------------------------------------------------------
    def __init__(self,nameList,fieldsList,inistored, inLen, off):
        'constructor of Header'
        #print '__init__ of Header'
          
        self.isStored=inistored #whether it is stored   
        self.lenOfTableNum=inLen#number of tables
        self.offsetOfBody=off
        self.tableNames=nameList
        self.tableFields=fieldsList

        #print "isStore is ",self.isStored," tableNum is ",self.lenOfTableNum," offset is ",self.offsetOfBody
        

    #-----------------------------
    # destructor of the class
    #-------------------------------
    def __del__(self):
        #print 'del Header'
        pass

        

    #-----------------------------
    # display the schema of all the tables in the schema file
    #----------------------------------------------------------
    def showTables(self):
        if self.lenOfTableNum>0:
            print "the length of tableNames is",len(self.tableNames)
            for i in range(len(self.tableNames)):
                print self.tableNames[i][0].strip()+' '*(10-len(self.tableNames[i][0].strip())),list(map(lambda x:x.strip(),self.tableFields[i]))
                #print self.tableFields[i]
            return True
        else:
            print 'There are not any table in database!'
            return False
