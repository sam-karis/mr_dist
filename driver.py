import logging
import os
import shutil
import string
from concurrent import futures

import grpc
from dotenv import load_dotenv

import word_count_pb2
import word_count_pb2_grpc

load_dotenv()


def get_bucket_id_ref(number_reduce_tasks):
    """
    Create an object with all potential bucket_ids given the number of reduce tasks
    Example {"a": "1", "b": "2", ...}
    """
    alp_num = string.digits + string.ascii_lowercase
    alp_num_length = len(alp_num)
    bucket_len = alp_num_length // number_reduce_tasks
    bucket_modulus = alp_num_length % number_reduce_tasks
    sub_buckets_list = []
    sub_count, lower_limit, upper_limit = 0, 0, 0
    for i in range(0, number_reduce_tasks):
        if sub_count < bucket_modulus:
            upper_limit += bucket_len + 1
        else:
            upper_limit += bucket_len
        sub_buckets_list.append(alp_num[lower_limit:upper_limit])
        lower_limit = upper_limit
        sub_count += 1

    res = {}
    for index, sub_bucket in enumerate(sub_buckets_list):
        for char in sub_bucket:
            res[char] = f"{index}"
    return res


def clean_up_work_space():
    # Remove intermediate and out directory when starting the driver
    logging.info("Cleaning ⌦⌦ up the workspace before starting the driver...")
    if os.path.exists("intermediate"):
        shutil.rmtree("intermediate/")
    if os.path.exists("out/"):
        shutil.rmtree("out/")


"""
Global Variable easy to track across multiple workers
Number of map and reduce tasks
This can be changed on updating the environment variable
"""
number_map_tasks = int(os.getenv("numberMapTasks", 3))
number_reduce_tasks = int(os.getenv("numberReduceTasks", 2))
# Bucket Id reference
bucket_ref = get_bucket_id_ref(number_reduce_tasks)
# Track assigned tasks
assigned_tasks = {"map": 0, "reduce": 0}
# Track Reduce Tasks
reduce_tasks_available = []
# Track task number
task_number = 0
# Track if bucket_id are provided
bucket_id_provided = False


class WordCount(word_count_pb2_grpc.WordCountServicer):

    @property
    def task(self):
        # Define task sent to
        return {
            "task_type": self.task_type,
            "task_number": task_number,
            "number_map_tasks": number_map_tasks,
            "number_reduce_tasks": number_reduce_tasks,
            "bucket_ref": bucket_ref,
        }

    def assign_task(self):
        """
        This determine the task to assign to the worker
        """
        global task_number
        if assigned_tasks["map"] == 0:
            self.task_type = "map"
            task_number = 0
        elif assigned_tasks["map"] < number_map_tasks:
            self.task_type = "map"
            task_number += 1
        elif reduce_tasks_available:
            self.task_type = "reduce"
            task_number = int(reduce_tasks_available.pop())
        else:
            self.task_type = "done"

        if self.task_type in ("map", "reduce"):
            assigned_tasks[self.task_type] += 1
            self.task_number = task_number

    def GetTask(self, request, context):
        global reduce_tasks_available
        global bucket_id_provided
        msg = "task"
        logger.info("Received a task request from worker🤝 🤝")
        if not bucket_id_provided and request.available_buckets:
            reduce_tasks_available = request.available_buckets
            bucket_id_provided = True
        if request.message == "active":
            self.assign_task()
        if self.task_type == "done":
            logger.info("No more tasks available")
            msg = "done"
        else:
            logger.info(f"Assigned {self.task_type} task to the worker!")
            logger.info(
                f"Driver has {number_map_tasks - assigned_tasks['map']} map and {number_reduce_tasks - assigned_tasks['reduce']} reduce remaining tasks "
            )

        return word_count_pb2.TaskResponse(message=msg, task=self.task)


def serve():
    clean_up_work_space()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    word_count_pb2_grpc.add_WordCountServicer_to_server(WordCount(), server)
    server.add_insecure_port("[::]:50051")
    logger.info("Starting driver.. 🏃‍♂️🏃‍♂️ !!")
    logger.info(
        f"Driver has {number_map_tasks} map and {number_reduce_tasks} reduce tasks"
    )
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(format="%(message)s")
    logger = logging.getLogger()
    logger.setLevel("INFO")
    serve()
