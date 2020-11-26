# GRPC WordCount Solution

To learn more about **gRPC** click [here](https://grpc.io/)

The solution provides:

- **Driver** - Receives task requests from the workers and determine the task to assign to the worker. If no more task is available it notify the worker.
- **Worker** - Initiate comunication with the `driver` by sending request for a task, execute the assigned task either `map or reduce`. After completing the task it notify the driver to be assigned another task untill all the tasks are complete thus exit.

### Driver and worker communication

- The **Driver** and **Worker** communicate to each other using **gRPC**
- The gRPC service, request and response format is defined in the [proto file](protos/word_count.proto)

- To Generate gRPC code used by our application run the command below.

```bash
$ python -m grpc_tools.protoc -I./protos --python_out=. --grpc_python_out=. ./protos/word_count.proto
```

- This will generate two files `word_count_pb2 and word_count_pb2_grpc` and to use the code on the program.

```python
import word_count_pb2
import word_count_pb2_grpc
```

#### To setup the environment (Python3)

- Check that python 3 is installed:

```bash
python --v
>> Python 3.8.6
```

- Create virtual environment and activate it

```bash
$ python -m venv rpc_venv
$ source rpc_venv/bin/activate
```

- Install dependencies:

```bash
$ pip install -r requirements.txt
```

- Configuring number of tasks edit `.env`

```
numberMapTasks >>>> Number of map tasks
numberReduceTasks >>>> Number of reduce tasks
```

#### To run the driver

This only need to be started on one terminal

```bash
python driver.py
```

#### To run the worker

- Worker can be started on multiple terminal windows

```bash
python worker.py
```

### Potential improvements

- Persist data instead of using python data structure
- Error handling
- Address potential race condition when running multiple workers
- Add tests

### ScreenShots

#### Program running

![alt Program running](screenshots/driver_workers.png?raw=true)

#### Directory structure after running program

![alt Directory structure](screenshots/dir_structure.png?raw=true)
