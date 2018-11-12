# - * - coding:utf8 - * - -
'''
################################################
1. 编写程序，模拟实现一下功能：
(1) 创建新的进程；
(2) 查看运行进程；
(3) 查看就绪进程；
(4) 查看阻塞进程；
(5) 阻塞某个进程;
(6) 显示全部进程;
(7) 唤醒进程;
(8) 撤销进程；
2. 创建5-10个进程，分别计算
先来先服务调度算法
短进程优先调度算法
优先调度算法
高相应比优先调度算法
下进程的执行顺序
以及平均周转时间和平均带权周转时间
'''
from monitor import *
from processing import *
from operations import *
import time


def main():
    monitor()
    main_menu()
    MEGA_TAG = 1
    while(MEGA_TAG not in ['0', '1']):
        MEGA_TAG = input('Please enter the select :').strip()

    if not int(MEGA_TAG):
        process_info = ProcessInfo()
        internal_memory = [ProcessType() for _ in range(process_info.amount)]
        while True:
            monitor()
            processing_operator_menu()
            choice = input('Please enter the select :').strip()
            monitor()
            if choice == '1':
                create_new_process(internal_memory, process_info)
            elif choice == '2':
                view_running_process(internal_memory, process_info)
            elif choice == '3':
                view_ready_process(internal_memory, process_info)
            elif choice == '4':
                view_blocking_process(internal_memory, process_info)
            elif choice == '5':
                block_specified_process(internal_memory, process_info)
            elif choice == '6':
                view_all_process(internal_memory, process_info)
            elif choice == '7':
                wakeup_specified_process(internal_memory, process_info)
            elif choice == '8':
                kill_specified_process(internal_memory, process_info)
            else:
                break
            time.sleep(2)

    else:
        job_message = JobMessage()
        jcbs = [JCB() for _ in range(job_message.job_number)]
        jcbs = read_jobs('input.txt', jcbs)
        jobs = ascending_by_arrive_time(jcbs)
        while True:
            monitor()
            calculating_operator_menu()
            choice = input('Please enter the select :').strip()
            monitor()
            job_message.reset()
            for i in range(job_message.job_number):
                jobs[i].reset()

            if choice == '1':
                first_come_first_served(job_message, jobs)
            elif choice == '2':
                priority_scheduling(job_message, jobs)
            elif choice == '3':
                short_job_first(job_message, jobs)
            elif choice == '4':
                highest_response_ratioNext(job_message, jobs)
            else:
                break
            time.sleep(3)

main()