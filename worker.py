
import glob
import logging
import os
import re
from collections import Counter

import grpc

import word_count_pb2
import word_count_pb2_grpc


class WordCount:
    def get_read_input_text(self):
        """
        This read text from all input files, remove punctuations and
        Create a list of all the words
        """
        corpus = ""
        files_list = glob.glob("inputs/*.txt")
        for file_name in files_list:
            with open(file_name, mode="r", encoding="utf-8") as f:
                data = f.read()
            corpus +=  data.lower() + "\n"
        corpus = re.sub(r"[^\w\s]", " ", corpus)
        corpus = re.sub(r"\_", "", corpus)
        corpus = corpus.split()
        return corpus

    def get_read_mapped_text(self):
        """
        This read text from all intermediate files with a specific bucket id
        Create a list of all the words
        """
        corpus = ""
        files_list = glob.glob(f"intermediate/*-{self.bucket_id}.txt")
        corpus = []
        for file_name in files_list:
            with open(file_name, mode="r", encoding="utf-8") as f:
                data = f.read()
                data = data.split()
            corpus.extend(data)
        return corpus

    def map_sub_tasks(self, number_map_tasks):
        # This get the text for the specifc map task
        self.corpus = self.get_read_input_text()
        corpus_num_length = len(self.corpus)
        map_len = corpus_num_length // number_map_tasks
        map_modulus = corpus_num_length % number_map_tasks
        sub_map_list = []
        sub_count, lower_limit, upper_limit = 0, 0, 0
        for i in range(0, number_map_tasks):
            if sub_count < map_modulus:
                upper_limit += map_len + 1
            else:
                upper_limit += map_len
            sub_map_list.append(self.corpus[lower_limit:upper_limit])
            lower_limit = upper_limit
            sub_count += 1
        return sub_map_list[self.task_number]

    def write_map_file(self, file_name, word_list):
        # write the content map task to a file
        if not os.path.exists("intermediate/"):
            os.makedirs("intermediate")
        with open(f"intermediate/{file_name}.txt", "w") as f:
            f.writelines(f"{word}\n" for word in word_list)

    def write_reducer_file(self):
        # write the content of a reduce task to a file
        if not os.path.exists("out/"):
            os.makedirs("out")
        with open(f"out/out-{self.bucket_id}.txt", "w") as f:
            f.writelines(f"{word} {count}\n" for word, count in self.counter.items())

    def map_words_bucket(self):
        """
        This map text of a specific map task to their bucket
        It use the first letter of the word and bucket_ref sent by the driver
        """
        res = {}
        for word in self.task_corpus:
            if res.get(self.bucket_ref[word[0]]) is None:
                res[self.bucket_ref[word[0]]] = []
                res[self.bucket_ref[word[0]]].append(word)
            else:
                res[self.bucket_ref[word[0]]].append(word)
        return res

    def execute_map_job(self, task):
        # Perform the logic of map task
        logger.info(f"Executing map task of id:  {task.task_number}")
        number_map_tasks = task.number_map_tasks
        self.task_number = task.task_number
        self.bucket_ref = task.bucket_ref
        self.task_corpus = self.map_sub_tasks(number_map_tasks)
        bucket_words = self.map_words_bucket()
        self.available_buckets = []
        for bucket, words in bucket_words.items():
            self.available_buckets.append(bucket)
            self.write_map_file(f"mr-{self.task_number}-{bucket}", words)

    def execute_reduce_job(self, task):
        # Perform the logic of reduce task
        logger.info(f"Executing reduce task of bucket:  {task.task_number}")
        self.bucket_id = str(task.task_number)
        self.mapped_corpus_list = self.get_read_mapped_text()
        self.counter = Counter(self.mapped_corpus_list)
        self.write_reducer_file()

    def execute_task(self, task):
        # Receive the task from the driver and dicide which task to excute
        if task.task_type == 0:
            self.execute_map_job(task)
        elif task.task_type == 1:
            self.execute_reduce_job(task)
        else:
            logger.info("Server has completed the tasks")

    def run(self):
        # NOTE(gRPC Python Team): .close() is possible on a channel and should be
        # used in circumstances in which the with statement does not fit the needs
        # of the code.
        with grpc.insecure_channel("localhost:50051") as channel:
            stub = word_count_pb2_grpc.WordCountStub(channel)
            msg = "active"
            self.available_buckets = []
            self.task_types = ["map", "reduce", "done"]
            while True:
                if not self.available_buckets:
                    logger.info("Requesting the first task..🙏 🙏")
                    response = stub.GetTask(word_count_pb2.TaskRequest(message=msg))
                    if response.task.task_type == 2:
                        logger.info("No Task available from the driver..")
                        break
                    logger.info(
                        f"Worker assigned {self.task_types[response.task.task_type]} task of id {response.task.task_number}..👍 👍"
                    )
                else:
                    response = stub.GetTask(
                        word_count_pb2.TaskRequest(
                            message=msg, available_buckets=self.available_buckets
                        )
                    )
                    logger.info(
                        f"Worker assigned {self.task_types[response.task.task_type]} task of id {response.task.task_number}..👍 👍"
                    )
                if response.message == "done" or response.task.task_type == 2:
                    logger.info("All tasks have been executed..💪 💪 💪")
                    break
                self.execute_task(response.task)


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(format="%(message)s")
    logger = logging.getLogger()
    logger.setLevel("INFO")
    instance = WordCount()
    instance.run()
