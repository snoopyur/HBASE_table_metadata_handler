"""
Restore HBase table from schema.json file without data
"""

import json

import os
import logging

import requests
import click


hbase_port = "<PORT_NUNMBER>" # TODO: Use your port
session = requests.session()
session.headers["Accept"] = "application/json"


def check_existence(target_hbase_rest_server: str, table: str):
    logging.info(f"Check if table exists: {table}!")
    logging.info("=============================================================")
    # Get all namespace and table name separate with ':'
    url = f"http://{target_hbase_rest_server}:{hbase_port}" # TODO: Change http to https if its needed
    response = session.get(url)
    response.raise_for_status()
    data = response.json()
    namespace, table_name = table.split(':')
    # Convert json to List from all table names
    table_names = [table['name'] for table in data['table']]

    # Observe table is exist or not
    if table in table_names:
        raise FileExistsError(f"Table already exists on {target_hbase_rest_server}: {table}")

    logging.info(f"Check namespace existence")
    logging.info("=============================================================")
    # Observe if namespace exists or not
    ns_url = f"{url}/namespaces"
    response = session.get(ns_url)
    response.raise_for_status()
    data = response.json()
    # If not exists, POST create namespace
    if namespace not in data:
        logging.info(f"Namespace:{namespace} not exists! Preparing...")
        logging.info("=============================================================")
        response = session.post(f"{ns_url}/{namespace}")
        response.raise_for_status()
        logging.info(f"Namespace:{namespace} was created!")


def get_backup(table: str, backup_dir: str, backup_name: str = 'latest') -> dict:
    """
    Retrieve table schema from backup schema.json by backup name or latest (by ctime).

    :param table:
    :param backup_dir:
    :param backup_name:
    :return:
    """
    logging.info(f"GET {table} backup schema metadata!")
    logging.info("=============================================================")
    if not os.path.isdir(backup_dir):
        raise FileNotFoundError(backup_dir)

    if not backup_name or backup_name == 'latest':
        backup_folders = {f.path: f.stat().st_ctime for f in os.scandir(backup_dir) if f.is_dir()}
        if not backup_folders:
            raise FileNotFoundError(f"Found no subfolders in {backup_dir}")
        path = sorted(backup_folders.keys(), key=lambda x: backup_folders[x])[-1]
        logging.info(f"Latest backup is {path}")
    else:
        path = os.path.join(backup_dir, backup_name)

    backup_file_name = "schema.json"
    if ':' in table:
        namespace, table_name = table.split(':')
        backup_file_path = os.path.join(path, namespace, table_name, backup_file_name)
    else:
        backup_file_path = os.path.join(path, table, backup_file_name)

    logging.info(f"Opening file {backup_file_path} ...")
    with open(backup_file_path, "r") as file:
        return json.load(file)


@click.command(context_settings={'show_default': True})
@click.option('-s', '--target_hbase_rest_server', type=str, help="Target HBase REST server", required=True)
@click.option('-t', '--table', type=str, help="HBase Keyspace:TableName", required=True, metavar='[KS:]TABLE')
@click.option('-n', '--backup_name', type=str, help="Backup to restore", default='latest')
@click.option('-d', '--backup_dir', type=str, help="Backup basedir", default="<DEFAULT_BACKUP_FOLDER>") # TODO: Fill in the default value!
def main(target_hbase_rest_server: str, backup_dir: str, backup_name: str, table: str):
    check_existence(target_hbase_rest_server=target_hbase_rest_server, table=table)
    logging.info(f"Create {table} from backup schema metadata on: {target_hbase_rest_server}!")
    logging.info("=============================================================")
    data = get_backup(table=table, backup_dir=backup_dir, backup_name=backup_name)
    url = f"http://{target_hbase_rest_server}:20550/{table}/schema" # TODO: Change http to https if its needed
    response = session.put(url, json=data)
    response.raise_for_status()
    logging.info("=============================================================")
    logging.info(f"DONE - Table {table}  is created on {target_hbase_rest_server}")


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main()