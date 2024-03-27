# HBase_table_metadata_handler

![img.png](img.png)

Hdfs metadata backuper and Table creator python code with HBase Rest API usage

First of all, I would like to explain why this code was created:

Some cases we try to reproduce the table without the data on HBASE clusters, but it's complicate because HBASE give 
snapshots, what's store keyspace, metadata and data. It's much more time if data size it's to large. I try to find on 
internet other solution how to get only keyspace:metadata combo, and the result was almost nothing.

My collage (Dr Illés Solt - thanks your help and advice ) mentioned Hbase Rest API contains a lot of options, where we 
figured up how to get a necessary data from actual keyspace:table combo and save it in json formatted file.   


# HBase Rest API setting

## Turn ON the REST Server using Cloudera Manager
link: https://docs.cloudera.com/runtime/7.2.17/accessing-hbase/topics/hbase-installing-rest-server-using-cm.html
Steps:
        1. Click the Clusters tab.
        2. Select Clusters > HBase.
        3. Click the Instances tab.
        4. Click Add Role Instance.
        5. Under HBase REST Server, click Select Hosts.
        6. Select one or more hosts to serve the HBase Rest Server role. Click Continue.
        7. Select the HBase Rest Server roles. Click Actions For Selected > Start.
    
Of course, there are other options for turning it on, but I won't discuss them now!

How works the API: https://docs.cloudera.com/runtime/7.2.17/accessing-hbase/topics/hbase-using-the-rest-api.html

Now we ready to use it! Congrats!

# Usage

Requirements:
- requests
- click

Please install necessary requirements from requirements.txt:
```
pip install -r requirements.txt
```

## Backup usage:
Use backup.py for this job.   
```python
@click.option('-s', '--target_hbase_rest_server', type=str, help="Target HBase REST server", required=True, 
              metavar="HOST")
@click.option('-d', '--backup_dir', type=str, help="Backup basedir", default="<DEFAULT_BACKUP_FOLDER>", metavar="PATH")
```

Flags:
```bash
'-s', '--target_hbase_rest_server', type=str, help="Target HBase REST server", required=True -> REQUIRED
'-d', '--backup_dir', type=str, help="Backup basedir", default="<DEFAULT_BACKUP_FOLDER>"
```
Please fill the missing variables settings:
- hbase_port = str -> line - required
- backup_dir default = str - good to fill it
- url = str http or https 

Usage with default backup_dir:
```bash
python3 backup.py --target_hbase_rest_server <FULL_HOSTNAME>
```
Done!

You find in backup folder schema.json file ```/<backup_dir>/<actual_datetime>/<keyspace>/<table_name>/
- regions.json
- schema.json

## Restore usage:
Use restore.py for this job.
```python
@click.option('-s', '--target_hbase_rest_server', type=str, help="Target HBase REST server", required=True)
@click.option('-t', '--table', type=str, help="HBase Keyspace:TableName", required=True, metavar='[KS:]TABLE')
@click.option('-n', '--backup_name', type=str, help="Backup to restore", default='latest')
@click.option('-d', '--backup_dir', type=str, help="Backup basedir", default="<DEFAULT_BACKUP_FOLDER>")
```

Flags:
```bash
'-s', '--target_hbase_rest_server', type=str, help="Target HBase REST server", required=True -> REQUIRED
'-t', '--table', type=str, help="HBase Keyspace:TableName", required=True, metavar='[KS:]TABLE' -> REQUIRED
'-n', '--backup_name', type=str, help="Backup to restore", default='latest' 
'-d', '--backup_dir', type=str, help="Backup basedir", default="<DEFAULT_BACKUP_FOLDER>")
```
Please fill the missing variables settings:
- hbase_port = str -> line - required
- backup_dir default = str - good to fill it
- url = str http or https 

Usage with default backup_dir and latest backup datetime:
```bash
python3 restore.py --target_hbase_rest_server <FULL_HOSTNAME> --table <KS:TABLE>
```

Good Luck and Hbase with you!
