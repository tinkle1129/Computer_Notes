#------------------------------------------
# mega_sfw.py
# author : Jingyu Han   hjymail@163.com
# modified by:Shuting Guo shutingnjupt@gmail.com
#----------------------------------------
# to process SELECT FROM WHERE clause
#------------------------------------------
import re
import mega_storage

class MegaSfw():
    
    #----------------------
    # constructor of the class
    # input
    #       schema_obj: object of schema
    #---------------------------------------    
    def __init__(self,schema_obj):
        self.schema_ptr=schema_obj
        


    #-------------------------------------------
    # to get the "select from where" result
    # Author: Shuting Guo shutingnjupt@gmail.com
    # input
    #   sql_str: select from where clause
    # note: we needs to create a tempory mega_storage object to get data from table
    #--------------------------------------------
    def process_sfw(self,sql_str):
        sql_str.strip()
        sel_list, from_list, where_list, where_list_condition, isFalse=self.parse_sfw(sql_str.strip())
        if isFalse:
            print 'WRONG SQL QUERY!'
            return

        #print self.schema_ptr.get_table_name_list()

        if from_list not in self.schema_ptr.get_table_name_list():
            print 'Cannot Find Table '+from_list+'!\n'
            return False
        else:
            dataObj = mega_storage.MegaStorage(from_list)
            Data_List=dataObj.get_record_list()
            Field_List=self.schema_ptr.viewTableStructure(from_list)
            if where_list=='':
                if sel_list=='*':
                    print '|'.join(Field_List)
                    for record in Data_List:
                        print '|'.join(record)
                else:
                    if sel_list not in Field_List:
                        print 'Cannot Find Field '+sel_list
                        return False
                    else:
                        Field_Index = Field_List.index(sel_list)
                        Output= list(map(lambda x:x[Field_Index],Data_List))
                        print '|'+sel_list+'|'
                        for record in Output:
                            print '|'+record+'|'
            else:
                if where_list not in Field_List:
                    print 'Cannot Find Field ' + where_list
                    return False
                else:
                    Field_Condition_Index = Field_List.index(where_list)
                    if sel_list=='*':
                        for record in Data_List:
                            if record[Field_Condition_Index] == where_list_condition:
                                print '|'+'|'.join(record)+'|'
                    else:
                        if sel_list not in Field_List:
                            print 'Cannot Find Field ' + sel_list
                            return False
                        else:
                            Field_Index = Field_List.index(sel_list)
                            #Output = list(map(lambda x: x[Field_Index], Data_List))
                            print '|' + sel_list + '|'
                            for record in Data_List:
                                if record[Field_Condition_Index] == where_list_condition:
                                    print '|' + record[Field_Index] + '|'


        #print sel_list,from_list,where_list,where_list_condition

    #----------------------------------------
    # parse the sfw clause into select list, from list and where list
    # Author: Shuting Guo shutingnjupt@gmail.com
    # input
    #       sql_str
    # output
    #       sel_list
    #       from_list
    #       where_list
    #------------------------------------------------------
    def parse_sfw(self,sql_str):
        sql_str.strip()
        sel_list='';from_list='';where_list='';where_list_condition=''
        ISFlase=0
        try:
            s = re.match(r'select (.*) from (.*) where (.*)', sql_str).groups()
            sel_list = s[0]
            from_list = s[1]
            where_list = s[2].split('=')[0]
            where_list_condition = s[2].split('=')[1]
        except:
            try:
                s = re.match(r'select (.*) from (.*)', sql_str).groups()
                sel_list = s[0]
                from_list = s[1]
                where_list = ''
                where_list_condition = ''
            except:
                ISFlase=1
        return sel_list,from_list,where_list,where_list_condition,ISFlase

