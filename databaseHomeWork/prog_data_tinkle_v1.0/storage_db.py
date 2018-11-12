#-----------------------------------------------------------------------
# storage_db.py
# Author: Shuting Guo shutingnjupt@gmail.com
#-----------------------------------------------------------------------
# the module is to store tables in files
# Each table is stored in a separate file with the suffix ".dat".
# For example, table named moviestar is stored in file moviestar.dat 
#-----------------------------------------------------------------------

#struct of file is as follows, each block is 4096
'''
block_0|block_1|...|block_n
'''
from common_db import BLOCK_SIZE


#structure of block_0, which stores the meta information and field information
'''
block_id                                # 0
number_of_dat_blocks                    # at first it is 0 because there is no data in the table
number_of_fields or number_of_records   # the total number of fields for the table
field_0_name                            #10bytes
field_0_type                            #4bytes,0->str,1->varstr,2->int,3->bool
field_0_lenth                           #4 bytes
field_1_name
field_1_type
...
'''





#structure of data block, whose block id begins with 1
'''
block_id       
number of records
record_0_offset         # it is a pointer to the data of record
record_1_offset
...
record_n_offset
....
free space
...
record_n
...
record_1
record_0
'''

#structre of one record
'''
pointer                     #offset of table schema in block id 0
length of record            # including record head and record content
time stamp of last update  # for example,1999-08-22
field_0_value
field_1_value
...
field_n_value
'''


import struct
import os
import ctypes

#--------------------------------------------
# the class can store table data into files
# functions include insert, delete and update
#--------------------------------------------

class Storage(object):

    #------------------------------
    #constructor of the class
    # input:
    #       tablename
    #-------------------------------------   
    def __init__(self,tablename): #there must be a self argument for python even if
        #print "__init__ of ",Storage.__name__,"begins to execute"
        tablename.strip()

        self.Free_Space_Number = 0
        self.Free_Space = []
        self.record_list = []
        self.record_Position =[]
        if  not os.path.exists('database/'+tablename+'.dat'):# the file corresponding to the table does not exist
            print 'table file '+tablename+'.dat does not exists'
            self.f_handle=open('database/'+tablename+'.dat','wb+')
            self.f_handle.close()
            print tablename+'.dat has been created'

        self.f_handle = open('database/'+tablename + '.dat', 'rb+')
        print 'table file ' + tablename + '.dat has been opened'
        self.open = True

        self.dir_buf = ctypes.create_string_buffer(BLOCK_SIZE)
        self.f_handle.seek(0)
        self.dir_buf = self.f_handle.read(BLOCK_SIZE)

        self.dir_buf.strip()
        my_len = len(self.dir_buf)
        self.field_name_list = []
        beginIndex = 0

        if my_len == 0:  # there is no data in the block 0, we should write meta data into the block 0
            self.num_of_fields = raw_input("please input the number of feilds in table " + tablename + ":")
            if int(self.num_of_fields) > 0:

                self.dir_buf = ctypes.create_string_buffer(BLOCK_SIZE)
                self.block_id = 0
                self.data_block_num =0
                struct.pack_into('!iii', self.dir_buf, beginIndex, 0, 0,
                                     int(self.num_of_fields))  # block_id,number_of_data_blocks,number_of_fields

                beginIndex = beginIndex + struct.calcsize('!iii')

                # the following is to write the field name,field type and field length into the buffer in turn
                for i in range(int(self.num_of_fields)):
                    field_name = raw_input("please input the name of field " + str(i) + " :")
                    if len(field_name) < 10:
                        field_name = ' ' * (10 - len(field_name.strip())) + field_name
                    flag=1
                    while(flag):
                        field_type = raw_input(
                                "please input the type of field(0, str; 1, varstr; 2, int; 3, boolean) " + str(i) + " :")
                        if int(field_type) in [0,1,2,3]:
                            flag=0

                    field_length = raw_input("please input the length of field " + str(i) + " :")
                    temp_tuple = (field_name, int(field_type), int(field_length))
                    self.field_name_list.append(temp_tuple)


                    struct.pack_into('!10sii', self.dir_buf, beginIndex, field_name, int(field_type),
                                         int(field_length))
                    beginIndex = beginIndex + struct.calcsize('!10sii')
                    struct.pack_into('!i',self.dir_buf,BLOCK_SIZE-struct.calcsize('!i'),int(0))

                # write the buffer into file
                self.f_handle.seek(0)
                self.f_handle.write(self.dir_buf)
                #print 'data has been written to file'
                self.f_handle.flush()
        else:  # there is something in the file

            self.block_id, self.data_block_num, self.num_of_fields = struct.unpack_from('!iii', self.dir_buf, 0)
            self.Free_Space_Number = struct.unpack_from('!i',self.dir_buf,BLOCK_SIZE-struct.calcsize('!i'))[0]
            if self.Free_Space_Number>0:
                for i in range(self.Free_Space_Number):
                    self.Free_Space.append(struct.unpack_from('!ii',self.dir_buf,BLOCK_SIZE-
                                                                  struct.calcsize('!i')-struct.calcsize('!ii')*
                                                                  (i+1)))
            #print self.Free_Space


            #print 'Free Space Number is ', self.Free_Space_Number
            #print 'number of fields is ', self.num_of_fields
            #print 'data_block_num', self.data_block_num
            beginIndex = struct.calcsize('!iii')

            # the followins is to read field name, field type and field length into main memory structures
            for i in range(self.num_of_fields):
                field_name, field_type, field_length = struct.unpack_from('!10sii', self.dir_buf,
                                                                              beginIndex + i * struct.calcsize(
                                                                                  '!10sii'))  # i means no memory alignment

                temp_tuple = (field_name, field_type, field_length)
                self.field_name_list.append(temp_tuple)
                #print "the " + str(i) + "th field information is ", temp_tuple

        Dict_ = {0: 's', 1: 's', 2: 'i', 3: '?'}
        '''
        0, str;
        1, varstr;
        2, int;
        3, boolean
        '''
        self.EncodeMethod = []
        for i in range(int(self.num_of_fields)):  # Output: field information
            if self.field_name_list[i][1] == 0 or self.field_name_list[i][1] == 1:
                self.EncodeMethod.append(str(self.field_name_list[i][2]) + Dict_[self.field_name_list[i][1]])
            else:
                self.EncodeMethod.append(Dict_[self.field_name_list[i][1]])

        self.EncodeFormat = '!' + ''.join(self.EncodeMethod)
        self.record_list = []
        self.record_Position = []

        Flag = 1
        while (Flag <= self.data_block_num):
            self.f_handle.seek(BLOCK_SIZE * Flag)
            self.active_data_buf = self.f_handle.read(BLOCK_SIZE)
            self.block_id, self.Number_of_Records = struct.unpack_from('!ii', self.active_data_buf, 0)
            #print 'Block_ID=%s,   Contains %s data' % (self.block_id, self.Number_of_Records)
            self.offsetList = []
            if self.Number_of_Records > 0:
                for i in range(self.Number_of_Records):
                    if (Flag, i) not in self.Free_Space:
                        self.record_Position.append((Flag, i))
                        self.offsetList.append(struct.unpack_from('!i', self.active_data_buf,
                                                                      struct.calcsize('!ii') + i * struct.calcsize(
                                                                          '!i'))[0])
            #print self.offsetList
            for idx in self.offsetList:
                self.record_list.append(
                        struct.unpack_from(self.EncodeFormat, self.active_data_buf, idx + struct.calcsize('!ii10s')))
            Flag += 1

    def datFlush(self,position,content):
        self.f_handle.seek(position)
        self.f_handle.write(content)
        self.f_handle.flush()

    def create_new_block(self):
        self.active_buf = ctypes.create_string_buffer(struct.calcsize('!ii'))
        self.data_block_num+=1
        struct.pack_into('!ii',self.active_buf,0,0,self.data_block_num)
        self.datFlush(0,self.active_buf)

        self.block_id=self.data_block_num
        self.Number_of_Records=0
        self.active_buf = ctypes.create_string_buffer(struct.calcsize('!ii'))
        struct.pack_into('!ii',self.active_buf,0,self.block_id,self.Number_of_Records)
        self.datFlush(self.data_block_num*BLOCK_SIZE,self.active_buf)

    # --------------------------------
    # to insert a record into table
    # -------------------------------
    def insert_record(self,input_record):
        insert_record=[]
        for i in range(int(self.num_of_fields)):
            if self.field_name_list[i][1]==2:
                insert_record.append(int(input_record[i].strip()))
            elif self.field_name_list[i][1]==3:
                insert_record.append(bool(input_record[i].strip()))
            else:
                if len(input_record[i])<self.field_name_list[i][2]:
                    insert_record.append(' ' * (10 - len(input_record[i].strip())) + input_record[i].strip())


        record_content_len = struct.calcsize(self.EncodeFormat)
        record_head_len = struct.calcsize('!ii10s')
        record_len = record_head_len + record_content_len

        MAX_RECORD_NUM = (BLOCK_SIZE - struct.calcsize('!i') - struct.calcsize('!ii')) / (
        record_len + struct.calcsize('!i'))

        #print 'Each Block Contains at most %s records' %(MAX_RECORD_NUM)
        self.record_list.append(insert_record)
        if len(self.Free_Space):
            self.record_Position.append(self.Free_Space[-1])
            self.Free_Space.remove(self.Free_Space[-1])
            self.Free_Space_Number=self.Free_Space_Number-1
        else:
            if not len(self.record_Position):
                self.data_block_num+=1
                self.record_Position.append((1,0))
            else:
                last_Position = self.record_Position[-1]
                if last_Position[1] == MAX_RECORD_NUM-1:
                    self.record_Position.append((last_Position[0]+1,0))
                    self.data_block_num+=1

                else:
                    self.record_Position.append((last_Position[0], last_Position[1]+1))
        print self.record_Position[-1]


    def getfilenamelist(self):
        return self.field_name_list

    def show_table_data(self):
        #self.get_table_data()
        #print len(self.record_list)
        print '|'.join(map(lambda x:x[0].strip(),self.field_name_list))
        for idx in self.record_list:
            tmp=[]
            for j in idx:
                try:
                    tmp.append(j.strip('\x00'))
                except:
                    tmp.append(str(j))
            print '|'.join(tmp)
        #print self.record_list[0][0].strip('\x00')


    # --------------------------------
    # to delete all the data in the table
    # input
    #
    # output
    #       True or False
    # -----------------------------------
    def delete_all_data(self):
        if self.data_block_num==0:
            return False
        data = ctypes.create_string_buffer((self.data_block_num+1)*BLOCK_SIZE)
        self.f_handle.seek(0)
        self.f_handle.write(data)
        self.data_block_num=0
        self.Free_Space_Number=0
        self.Free_Space=[]
        return True

    #--------------------------------
    # to delete one record from the  table
    # input
    #
    #       record_tuple: the tuple of record to be deleted
    # output
    #       True or False
    #-----------------------------------
    def delete_table_data(self,record_tuple):
        try:
            Delete_Index=list(map(lambda x:x[0].strip(),self.field_name_list)).index(record_tuple[1])
        except:
            print 'Wrong Input!'
            return
        tmp_Record_Message=zip(self.record_list,self.record_Position)
        self.record_list=[]
        self.record_Position=[]
        self.Free_Space=[]
        for record in tmp_Record_Message:
            tmp_record = record[0][Delete_Index]
            try:
                tmp_record=tmp_record.split()[0]
            except:
                pass
            if tmp_record != record_tuple[0]:
                self.record_list.append(record[0])
                self.record_Position.append(record[1])
            else:
                print 'Hello'
                self.Free_Space.append(record[1])
        self.Free_Space_Number=len(self.Free_Space)

    def getRecord(self):
        return self.record_list

    def getFieldName(self):
        return list(map(lambda x:x[0],self.field_name_list))

    def __del__(self):  # write the metahead information in head object to file
        record_content_len = struct.calcsize(self.EncodeFormat)
        record_head_len = struct.calcsize('!ii10s')
        record_len = record_head_len + record_content_len

        self.data_buf = ctypes.create_string_buffer(BLOCK_SIZE*(self.data_block_num+1))

        #print self.data_block_num,self.num_of_fields
        #block_id,data_block_num,num_of_fields
        struct.pack_into('!iii',self.data_buf,0,0,int(self.data_block_num),int(self.num_of_fields))
        #print self.num_of_fields,self.field_name_list


        #field name,type,len
        for i in range(int(self.num_of_fields)):
            struct.pack_into('!10sii',self.data_buf,struct.calcsize('!iii')+struct.calcsize('!10sii')*i,
                             self.field_name_list[i][0],self.field_name_list[i][1]
                             ,self.field_name_list[i][2])

        Begin_Index=BLOCK_SIZE-struct.calcsize('!i')
        #free_space_number
        struct.pack_into('!i',self.data_buf,Begin_Index,self.Free_Space_Number)
        #free_space
        for i in range(len(self.Free_Space)):
            struct.pack_into('!ii',self.data_buf,Begin_Index-struct.calcsize('!ii')*(i+1),
                             self.Free_Space[i][0],self.Free_Space[i][1])

        Flag=1
        while(Flag<=self.data_block_num):
            count=-1
            for idx,idj in zip(self.record_Position,self.record_list):
                if idx[0]==Flag:
                    count = idx[1]
                    offset = BLOCK_SIZE*Flag+struct.calcsize('!ii')+idx[1]*struct.calcsize('!i')
                    beginindex = BLOCK_SIZE-record_len*(idx[1]+1)
                    #offset
                    struct.pack_into('!i',self.data_buf,offset,beginindex)
                    record_schema_address = struct.calcsize('!iii')  # offset in  the block 0
                    update_time = '2016-11-16'  # update time
                    #message
                    struct.pack_into('!ii10s',self.data_buf,BLOCK_SIZE*Flag+beginindex,record_schema_address,record_len,update_time)
                    for i in range(len(idj)):
                        struct.pack_into('!' + self.EncodeMethod[i], self.data_buf,BLOCK_SIZE*Flag+beginindex+record_head_len + struct.calcsize(self.EncodeFormat)
                                        - struct.calcsize('!' + ''.join(self.EncodeMethod[i:])), idj[i])
            #block_id,num_of_records
            struct.pack_into('!ii', self.data_buf, BLOCK_SIZE*Flag, Flag,count+1)
            Flag+=1
        self.f_handle.seek(0)
        self.f_handle.write(self.data_buf)
        self.f_handle.flush()

