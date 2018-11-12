#Author: Shuting Guo shutingnjupt@gmail.com
#Time: 2017/01/05 

本系统为2016-2017南京邮电大学第一学期研究生数据库系统课程的期末作业

主要依赖的包有struct re ply types itertools

head_db为schema_db.py的头文件
schema_db.py | mega_storage.py | mega_sfw.py 共同组成了本系统的第一部分
schema_db.py | storage.py | common_db.py | parser_db.py | query_plan_db.py | lex_db.py 共同组成了本系统的第二部分
main_db_tinkle.py为主函数
all.sch文件为数据库文件，存储了表名和表结构
database文件夹中存储表的内容

在运行前，需要使用者根据自己的系统情况对main_db_tinkle.py的monitor()函数中的清屏操作进行修改

因本人经验不足，本系统对用户的输入要求较为严格，尤其在第二部分，用户输入内容可能有大小限制，但在运行中并没有提示，需要用户牢记列的字段长度（如小于10个字符）。

因英文烂的缘故，在主代码中注释非常少，附一份数据库实验报告，里面用中文说明了系统从产生到执行过程中每个函数的主要内容，供参考。

附演示视频，使用者可以根据视频进行操作测试。
从第一部分切换到第二部分时，先使用清空函数，再需要手动将database中的全部文件删除，否则可能会出现报错问题。

如有需要帮助或完善的，请与本人联系。