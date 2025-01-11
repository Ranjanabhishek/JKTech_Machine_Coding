import asyncio
import threading
from collections import defaultdict
from collections import deque
import time

"""
Working of Task Scheduler:
  - So we have used here the threading in python with the help of asyncio concurrency and for that
    await functionality 

  - For Pushing task for execution we have used Graph BFS (Breadth First Search) Algorithm
        - We have created a queue and indegree list (this will contains the count of dependencies of each task)
        - Also we have created Graph for maintaing the task and dependency
            for example:
               graph--> {'1': [[]], '2': [[1]], '3': [[]], '4': [[2, 3]]}
               indegree----> {'1': 0, '2': 1, '3': 0, '4': 2}
            
            once we find task with 0 dependecies we will push that task with time_duration in queue
            so as we know there is limit of max_concurrency of 2 so we will traverse the loop for 2 time in single 
            time and push those task to thread with using await functionality, also once task gets executed 
            it will reduce other's dependency count so when any task with dependecny gets 0 it will get pushed in queue.

            why I have used await ?? 
               As we know in python mutlithreaing is not supported due GIL (Global interpreter lock) so to avoid
               this I have async/await and it will run in a single thraeded environment and it will wait other task 
               to complete by doing this way we can achieve concurrency.
            
"""

class Task:
    
    def __init__(self, task, time_duration, task_dependencies = []):
        self.task = task
        self.time_duration = time_duration
        self.task_dependencies = task_dependencies
        self.total_tasks = []
        
    def combine_all_tasks(self):
        # import uuid
        # self.id = uuid.uuid4()
        self.total_tasks.append([self.task, self.time_duration, self.task_dependencies])
        return self.total_tasks

class TaskScheduler:
    async def run(self, tasks):
        try:
            print('Task: {}: started'.format(tasks[0][0]))
            execution_time = int(tasks[0][1])
            time.sleep(execution_time)
        except Exception as e:
            print('Error while running task in async way, please find the erorr: {}'.format(e))    

    async def execute_tasks(self, all_tasks, max_concurrency = 2):

        try:
            queue = deque() #using queue
            graph = {}  # for making graph of task and dependency
            task_time = {}
            indegree_list = []
            
            for data in all_tasks:
                task_data = data[0]
                task = task_data[0]
                time = task_data[1]

                if task not in task_time.keys():
                    task_time[task] = time

                dependency = task_data[2]
                graph[task] = []
                graph[task].append(dependency)  #creating graph

            indegree = {}

            for key, value in graph.items():  #creating indegree items
                indegree[key] = len(value[0])

            for key, val in indegree.items():
                if val == 0 and len(queue)<=max_concurrency:
                    queue.append((key, val))
            
            task_to_execute = []
            execute = 0
            
            while queue:
                size = len(queue)
                for i in range(0, 2):
                    (key, val) = queue.popleft()
                    time_to_execute = task_time[key]
                    task_to_execute.append([key, time_to_execute])
                    await self.run(task_to_execute)
                    execute+=1
                    print('Task : {} Completed'.format(key))
                    task_to_execute=[]

                    for task, dependency in graph.items():
                        search = dependency[0]
                        if key in search:

                            indegree[task] = indegree[task] - 1
                            if indegree[task] == 0:
                               queue.append((task, indegree[key]))  
            
            if execute == len(all_tasks):
                print('All Task completed successfully')
            else:
                print('Task are getting executed please wait')
        except Exception as e:
            print('Error while execting tasks in parallel, please find the errir: {}'.format(e))


all_tasks = []

task1 = Task('1', '2', [])
all_tasks.append(task1.combine_all_tasks())

task2 = Task('2', '5', [1])
all_tasks.append(task2.combine_all_tasks())

task3 = Task('3', '3', [])
all_tasks.append(task3.combine_all_tasks())

task4 = Task('4', '4', [2,3])
all_tasks.append(task4.combine_all_tasks())

scheduler = TaskScheduler()
asyncio.run(scheduler.execute_tasks(all_tasks,2))
