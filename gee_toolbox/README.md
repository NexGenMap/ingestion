###### gee_toolbox ( _this is a work in progress_ )
___
 
`gee_toolbox` is convenience module and command line tool for GEE.  Note if you have earthengine installed you already have access to the [earthengine CLI](https://developers.google.com/earth-engine/command_line]). In most respects the [earthengine CLI](https://developers.google.com/earth-engine/command_line]) is better and more complete.  

Use cases for this CLI/Module are:

###### CLI

* [Task management](#task-cli): easily list, cancel, summarize tasks
* [User management](#user-management): manage multiple users on one machine. (note: you probably shouldn't use this)

###### PYTHON MODULE

* [gee.init](#gee-init): initialization that automatically uses service accounts (when an ENV var is present)
* [Asset management](#asset-management): a python wrapper for the `earthengine-cli` that provides
  * `asset_exists()`!!!
  * `asset_cp/mv/list()` 


##### INSTALL

```bash
# pip (testpypi)
pip install -U -i https://testpypi.python.org/pypi gee_toolbox

# github
$ git https://github.com/wri/gee_toolbox
$ cd gee_toolbox/
$ pip install . 
```
___

<a name='asset-management'></a>
##### ASSET MANAGEMENT 

These methods 

```python
def asset_exists(path,full_path=False):
def asset_mv(from_path,to_path,full_path=False):
def asset_cp(from_path,to_path,full_path=False):
def asset_list(path,full_path=False):
```

do exactly what you think they do. If `full_path=False` and the environment variable `EE_PROJECT_ROOT` exists, it will prefix the file paths with `$EE_PROJECT_ROOT/`.


`gee.asset_cmd` is a more general cli wrapper that is used internally for the above methods but could be used to implement other cli methods. As an example, `gee.asset_cmd('cp',[from_path,to_path])` is the same as `earthengine cp from_path to_path` (Note however for copy you would just use `gee.asset_cp`).

___

<a name='task-cli'></a>
##### COMMAND LINE

```bash
$ gee -h
usage: gee.py [-h] {status,summary,cancel,tasks,user} ...

GEE HELPER

positional arguments:
  {status,summary,cancel,tasks,user}
    status              GEE Task Status (consider using gee.summary)
    summary             GEE Task Summary
    cancel              Cancel Tasks
    tasks               Prints summary of recent tasks
    user                User Info

optional arguments:
  -h, --help            show this help message and exit
```
____



```bash
$ gee summary -h
usage: gee.py summary [-h] [-d DETAILED] [-p PROPS] task_id

positional arguments:
  task_id               gee-task-id

optional arguments:
  -d DETAILED, --detailed DETAILED
                        include details
  -p PROPS, --props PROPS
                        comma seperated string of properties

```
____



```bash
$ gee tasks -h
usage: gee.py tasks [-h] [-n NUM] [-t TASK_ID] [-d DESCRIPTION] [-s STATES]
                    [-p PROPS]

optional arguments:
  -n NUM, --num NUM     number of tasks to print
  -t TASK_ID, --task_id TASK_ID
                        any portion of gee-task-id
  -d DESCRIPTION, --description DESCRIPTION
                        any portion of gee-task-description
  -s STATES, --states STATES
                        commas seperated state names or one of
                        all|opentasks|finished
  -p PROPS, --props PROPS
                        commas seperated property names to display
```
____



```bash
$ gee cancel -h
usage: gee.py cancel [-h] [-t TASK_ID] [-d DESCRIPTION] [-s STATES]

optional arguments:
  -t TASK_ID, --task_id TASK_ID
                        any portion of gee-task-id
  -d DESCRIPTION, --description DESCRIPTION
                        any portion of gee-task-description
  -s STATES, --states STATES
                        commas seperated state names or one of
                        all|opentasks
```

___

<a name='gee-init'></a>
##### SERVICE_ACCOUNT

To use a service account simply set a `SERVICE_ACCOUNT` environment variable 

```bash
$ export SERVICE_ACCOUNT=yourserviceaccount@project_id.iam.gserviceaccount.com
```

and `gee.init()` will use the account to initialize earthengine.

___

<a name='user-management'></a>
##### USERS
This module contains methods for (the potentially unsafe but convenient practice of) handling multiple GEE accounts on the same computer. For this to work simply create a sub-directories in your `.config/earthengine` for each user containing their crediential file. The system will then create a `current_user.txt` file and `credentials-last` (backup) file. Your `.config/earthengine` directory will look like this:

```bash
$ tree <USER_HOME>/.config/earthengine
<USER_HOME>/.config/earthengine
├── username1
│   └── credentials
├── username2
│   └── credentials
├── credentials
├── credentials-last
├── current_user.txt
├── username3
│   └── credentials
```

```bash
$ gee user -h
usage: gee.py user [-h] [-n NEW]

optional arguments:
  -n NEW, --new NEW  new username
```