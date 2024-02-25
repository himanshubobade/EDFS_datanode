# hdfs-emulation
Emulating Hadoop Distributed File System (HDFS)

## Before you start
Go to the directory where you download this repo, and\
```cd hdfs-emulation```

## Environment setup
you could use either one of the methods below:
### 1. Pyhton virtual environment
```python3 -m venv ~/ds551```\
```source ~/ds551/bin/activate```\
```pip3 install -r requirements.txt```
### 2. manually install the packages in requirements.txt

## Run
First start up the namenodes and datanodes

### namenode
```python3 namenode.py```

### datanodes
```python3 datanode_A.py```\
```python3 datanode_B.py```\
```python3 datanode_C.py```

### client CLI
example:
```python3 edfs.py -ls /```

## Web Interface
After starting up the namenode and datanodes, go to the web directory\
```cd web```
### start up backend API
```python3 backend.py```\

### access webpage
[http://127.0.0.1:8080](http://127.0.0.1:8080)
