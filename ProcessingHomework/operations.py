# - * - coding:utf8 - * - -
# case 1 : 创建新的进程
def create_new_process(internal_memory, process_info):
    print('#              1. Create new process                 #')
    print('#----------------------------------------------------#')

    if(process_info.num == process_info.amount):
        print('#    Memmory is up to  %d #' % process_info.amount)
        print('#----------------------------------------------------#')
    else:
        process_info.num += 1
        i = 0
        while(i < process_info.amount and internal_memory[i].state!=0):
            i += 1
        print('#----------------------------------------------------#')
        internal_memory[i].pid = int(input('Please enter new process PID :').strip())
        internal_memory[i].priority = int(input('Please enter new process priority :').strip())
        internal_memory[i].size = int(input('Please enter new process size :').strip())
        internal_memory[i].info = input('Please enter new process information :').strip()
        internal_memory[i].state = 1

def view_process(internal_memory, process_info, state):
    state_map = {1:'active',
                 2:'blocking',
                 3:'ready',
                 4:''}

    flag = True
    for i in range(process_info.amount):
        if internal_memory[i].state == state or (internal_memory[i].state > 0 and state == 4):
            print('Process %d is %s' % (i, state_map[internal_memory[i].state]))
            print('The PID of Process %d is %d' % (i, internal_memory[i].pid))
            print('The priority of Process %d is %d' % (i, internal_memory[i].priority))
            print('The size of Process %d is %d' % (i, internal_memory[i].size))
            print('The information of Process %d is %s' % (i, internal_memory[i].info))
            print('#----------------------------------------------------#')
            flag = False

    if flag:
        print('      There are not any %s process at all       !' % state_map[state])
        print('#----------------------------------------------------#')

# case 2 : 查看运行进程
def view_running_process(internal_memory, process_info):
    print('#              2. View running process               #')
    print('#----------------------------------------------------#')

    view_process(internal_memory, process_info, 1)

# case 3 : 查看就绪进程
def view_ready_process(internal_memory, process_info):
    print('#              3. View  ready  process               #')
    print('#----------------------------------------------------#')

    view_process(internal_memory, process_info, 3)

# case 4 : 查看阻塞进程
def view_blocking_process(internal_memory, process_info):
    print('#              4. View blocking process              #')
    print('#----------------------------------------------------#')

    view_process(internal_memory, process_info, 2)

# case 5 : 阻塞指定进程
def block_specified_process(internal_memory, process_info):
    print('#              5. block specified process            #')
    print('#----------------------------------------------------#')

    pid = int(input('Please enter process PID :').strip())
    flag = 2
    for i in range(process_info.amount):
        if internal_memory[i].pid == pid:
            flag = 1
            if internal_memory[i].state == 1:
                internal_memory[i].state = 2
                print('Blocking %d process succeeded.' % pid)
                flag = 0
            elif internal_memory[i].state == 2:
                print('%d process is blocked before.' % pid)
            elif internal_memory[i].state == 3:
                print('%d process is ready before.' % pid)
            else:
                flag = 2
    if flag:
        if flag == 2:
            print('There doesn\'t exists %d process.' % pid)
        print('Blocking %d process failure.' % pid)

# case 6 : 显示所有进程
def view_all_process(internal_memory, process_info):
    print('#              6. View   all   process               #')
    view_process(internal_memory, process_info, 4)

# case 7 : 唤醒进程
# blocked -> ready
def wakeup_specified_process(internal_memory, process_info):
    print('#              7. Wakeup  specified  process         #')
    pid = int(input('Please enter process PID :').strip())
    flag = 2
    for i in range(process_info.amount):
        if internal_memory[i].pid == pid:
            flag = 1
            if internal_memory[i].state == 1:
                print('%d process is blocked before.' % pid)
            elif internal_memory[i].state == 2:
                internal_memory[i].state = 3
                print('Wakeup %d process succeeded.' % pid)
                flag = 0
            elif internal_memory[i].state == 3:
                print('%d process is ready before.' % pid)
            else:
                flag = 2
    if flag:
        if flag == 2:
            print('There doesn\'t exists %d process.' % pid)
        print('Blocking %d process failure.' % pid)

# case 8 : 杀死进程
def kill_specified_process(internal_memory, process_info):
    print('#              8. Kill  specified  process           #')
    pid = int(input('Please enter process PID :').strip())
    flag = 1
    for i in range(process_info.amount):
        if internal_memory[i].pid == pid:
            if internal_memory[i].state != 0:
                internal_memory[i].state = 0
                process_info.num -=1
                print('Kill %d process succeeded.' % pid)
                flag = 0
    if flag:
        print('There doesn\'t exists %d process.' % pid)
        print('Blocking %d process failure.' % pid)

# 读取进程
def read_jobs(file_path, jobs):
    idx = 0
    with open(file_path, 'r') as lines:
        print('job_name, arrive_time, run_time, priority, job_size')
        for line in lines:
            line = line.split()
            jobs[idx].job_name = line[0]
            jobs[idx].arrive_time = int(line[1])
            jobs[idx].run_time = int(line[2])
            jobs[idx].priority = int(line[3])
            jobs[idx].job_size = int(line[4])
            jobs[idx].start_time = 0
            jobs[idx].end_time = 0
            jobs[idx].turn_over_time = 0
            jobs[idx].use_weight_turn_over_time = 0
            jobs[idx].process_status = 'wait'
            print('job_name=%s, arrive_time=%d, run_time=%d, priority=%d, job_size=%d'
                  %(jobs[idx].job_name, jobs[idx].arrive_time, jobs[idx].run_time,
                    jobs[idx].priority, jobs[idx].job_size))
            idx += 1
    print('Load Jobs Done.')
    return jobs


# 比较各个进程之间的到达时间,按升序排列
def ascending_by_arrive_time(jobs):
    return sorted(jobs, key=lambda x:x.arrive_time)

# 计算周转时间
# weight = False 平均周转时间 ; True 平均带权值周转时间
def turn_over_time_count(job_message, jobs, weight = False):
    sum_ = 0.0
    for i in range(job_message.job_number):
        if weight:
            sum_ += jobs[i].use_weight_turn_over_time
        else:
            sum_ += jobs[i].turn_over_time
    return sum_ / job_message.job_number

# 打印进程
def print_jobs(job_message, jobs):
    print('Current Time is %d' % job_message.current_time)
    print('job_name, arrive_time, run_time, start_time, end_time, '
          'turn_over_time, use_weight_turn_over_time, process_status')
    for i in range(job_message.job_number):
        if jobs[i].process_status == 'finish':
            print (jobs[i].job_name, jobs[i].arrive_time,jobs[i].run_time,
                   jobs[i].start_time,jobs[i].end_time,jobs[i].turn_over_time,
                   jobs[i].use_weight_turn_over_time,jobs[i].process_status)
        elif jobs[i].process_status == 'run':
            print (jobs[i].job_name, jobs[i].arrive_time, jobs[i].run_time,
                   jobs[i].start_time, 'Running...', '',
                   '', jobs[i].process_status)
        else:
            print (jobs[i].job_name, jobs[i].arrive_time, jobs[i].run_time,
                   'Waiting...', '', '',
                   '', jobs[i].process_status)
    print('#----------------------------------------------------#')

def print_info(job_message, jobs):
    print('1. The sequence of process scheduling is')
    print(job_message.job_arrays)
    print('2. The mean turn over time count is ',turn_over_time_count(job_message, jobs))
    print('3. The mean weighted turn over time count is ',turn_over_time_count(job_message, jobs, True))
    print('#----------------------------------------------------#')

# 算法共同循环遍历部分
def loop(job_message, jobs, i):
    jobs[i].start_time = job_message.current_time
    jobs[i].end_time = jobs[i].start_time + jobs[i].run_time
    jobs[i].turn_over_time = jobs[i].end_time - jobs[i].arrive_time
    jobs[i].use_weight_turn_over_time = float(jobs[i].turn_over_time) / jobs[i].run_time
    jobs[i].process_status = 'run'
    while True:
        if job_message.current_time == jobs[i].end_time:
            jobs[i].process_status = 'finish'
            job_message.finish_job += 1
            job_message.current_time -= 1
            return job_message, jobs
        else:
            job_message.current_time += 1


# 先来先服务调度算法
def first_come_first_served(job_message, jobs):
    print('#            1. First Come First Served              #')
    i = 0
    while(job_message.finish_job < job_message.job_number):
        if(job_message.current_time < jobs[0].arrive_time):
            print_jobs(job_message, jobs)
        else:
            job_message.job_arrays[i] = jobs[i].job_name
            job_message, jobs =  loop(job_message, jobs, i)
            i += 1
        job_message.current_time += 1
    print_info(job_message, jobs)

# 优先级调度算法
def priority_scheduling(job_message, jobs):
    print('#            2.   Priority Scheduling                #')
    i = 0
    while(job_message.finish_job < job_message.job_number):
        max_priority = 0.0
        index_priority = 0
        if(job_message.current_time < jobs[0].arrive_time):
            print_jobs(job_message, jobs)
        else:
            for j in range(job_message.job_number):
                if jobs[j].process_status != 'finish' and job_message.current_time > jobs[0].arrive_time:
                    job_message.priorities[j] = jobs[j].priority
                    if job_message.priorities[j] > max_priority:
                        max_priority = job_message.priorities[j]
                        index_priority = j
            job_message.job_arrays[i] = jobs[index_priority].job_name
            i += 1
            job_message, jobs = loop(job_message, jobs, index_priority)
        job_message.current_time += 1
    print_info(job_message, jobs)

# 短进程优先调度算法
def short_job_first(job_message, jobs):
    print('#            3.    Short Job First                   #')
    i, j = 0, 0
    while (job_message.finish_job < job_message.job_number):
        min_job = 100.0 # 最大优先权
        index_job = 0
        if (job_message.current_time < jobs[0].arrive_time):
            print_jobs(job_message, jobs)
        else:
            for k in range(job_message.job_number):
                if jobs[k].process_status != 'finish' and job_message.current_time > jobs[0].arrive_time:
                    job_message.priorities[k] = jobs[k].priority
                    if job_message.priorities[k] < min_job:
                        min_job = job_message.priorities[k]
                        index_job = k
            job_message.job_arrays[j] = jobs[index_job].job_name
            j += 1
            job_message, jobs = loop(job_message, jobs, index_job)
        job_message.current_time += 1
    print_info(job_message, jobs)

# 高相应比优先调度算法
def highest_response_ratioNext(job_message, jobs):
    print('#            4. Highest Response Ratio Next          #')
    i = 0
    while(job_message.finish_job < job_message.job_number):
        max_priority = 0.0
        index_priority = 0
        if(job_message.current_time < jobs[0].arrive_time):
            print_jobs(job_message, jobs)
        else:
            for j in range(job_message.job_number):
                if jobs[j].process_status != 'finish':
                    wait_time = job_message.current_time - jobs[j].arrive_time
                    job_message.priorities[j] = float(wait_time + jobs[j].run_time) / jobs[j].run_time
                    if job_message.priorities[j] > max_priority:
                        max_priority = job_message.priorities[j]
                        index_priority = j
            job_message.job_arrays[i] = jobs[index_priority].job_name
            i += 1
            job_message, jobs = loop(job_message, jobs, index_priority)
        job_message.current_time += 1
    print_info(job_message, jobs)
