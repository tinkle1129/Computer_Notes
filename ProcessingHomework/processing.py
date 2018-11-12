# - * - coding:utf8 - * - -
class ProcessInfo(object):
  def __init__(self, amount=5, num=0):
    '''
    :param amount:
    :param num:
    '''
    self.amount = amount
    self.num = num


class ProcessType(object):
  def __init__(self, pid=0, priority=0, size=0, state=0, info = ''):
    '''
    :param pid: 进程ID
    :param priority: 优先级
    :param size: 容量
    :param state: 状态
    :param info: 信息
    '''
    self.pid = pid
    self.priority = priority
    self.size = size
    self.state = state # 1: active 2: blocking 3: ready
    self.info = info

class JobMessage(object):
  def __init__(self,
               job_number = 5
               ):
    '''
    :param job_number:
    :param current_time:
    :param finish_job:
    :param job_arrays:
    :param prioritys:
    '''
    self.job_number = job_number
    self.current_time = 0
    self.finish_job = 0
    self.job_arrays = ['' for _ in range(job_number)]
    self.priorities = [0.0 for _ in range(job_number)]

  def reset(self):
    self.current_time = 0
    self.finish_job = 0
    self.job_arrays = ['' for _ in range(self.job_number)]
    self.priorities = [0.0 for _ in range(self.job_number)]


class JCB(object):
  def __init__(self,
               job_name = '',
               arrive_time = 0,
               run_time = 0,
               priority = -1,
               job_size = 0,
               start_time = 0,
               end_time = 0,
               turn_over_time = 0,
               use_weight_turn_over_time = 0,
               process_status = 'wait'
               ):
    '''
    :param job_name: 作业名
    :param arrive_time:  到达时间
    :param run_time: 需要运行时间
    :param priority: 优先级数
    :param job_size: 作业大小
    :param start_time: 开始时间
    :param end_time: 完成时间
    :param turn_over_time: 周转时间
    :param use_weight_turn_over_time: 带权周转时间
    :param process_status: 进程状态 wait run finish
    '''
    self.job_name = job_name
    self.arrive_time = arrive_time
    self.run_time = run_time
    self.priority = priority
    self.job_size = job_size
    self.start_time = start_time
    self.end_time = end_time
    self.turn_over_time = turn_over_time
    self.use_weight_turn_over_time = use_weight_turn_over_time
    self.process_status = process_status

  def reset(self):
    self.start_time = 0
    self.end_time = 0
    self.turn_over_time = 0
    self.use_weight_turn_over_time = 0
    self.process_status = 'wait'