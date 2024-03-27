"""
Backup HBase table schema for all non-system tables via HBase REST server
"""
import json
import sys
import os
import logging
from datetime import datetime

import click
import requests


session = requests.session()
session.headers["Accept"] = "application/json"

logging.basicConfig(level=logging.DEBUG)


def get_tables_name(url: str):
    response = session.get(url)
    response.raise_for_status()
    data = response.json()
    logging.info(f"Get tables names!")
    logging.info("=============================================================")

    # Convert json to List from all table names
    all_table = [table['name'] for table in data['table']]
    logging.info("Found %d tables", len(all_table))

    return all_table


def get_all_table_backup(all_table: list, backup_dir: str, url: str):
    time_stamp = datetime.now().strftime("%Y-%m-%d")
    # Iterate all namespace/table_name
    exit_status = 0
    logging.info(f"Get all tables metadata!")
    logging.info("=============================================================")
    for name in all_table:
        logging.info("Processing table %s ...", name)
        logging.info("=============================================================")
        if ':' in name:
            namespace, table_name = name.split(':')
            path = os.path.join(backup_dir, time_stamp, namespace, table_name)
            logging.info(f"Backup {namespace}/{table_name} done")
            logging.info("=============================================================")
        else:
            namespace, table_name = None, name
            path = os.path.join(backup_dir, time_stamp, table_name)
            logging.info(f"Backup {table_name} ready")
            logging.info("=============================================================")
        if not os.path.isdir(path):
            os.makedirs(path)

        # Save schema backup file
        try:
            logging.info(f"Get all schema metadata!")
            logging.info("=============================================================")
            schema_url = f"{url}/{name}/schema"
            response = session.get(schema_url)
            response.raise_for_status()
            schema_data = response.json()
            schema_name = os.path.join(path, "schema.json")
            with open(schema_name, 'w') as file:
                json.dump(schema_data, file, indent=2)
            logging.info(f"Backup {name} table schema backup ready")
            logging.info("=============================================================")
        except requests.HTTPError:
            logging.exception("Failed to fetch schema of " + name)
            exit_status = 1

        # Save region backup file
        try:
            logging.info(f"Get all region metadata!")
            logging.info("=============================================================")
            region_url = f"{url}/{name}/regions"
            response = session.get(region_url)
            response.raise_for_status()
            region_data = response.json()
            region_name = os.path.join(path, "regions.json")
            with open(region_name, 'w') as file:
                json.dump(region_data, file, indent=2)
        except requests.HTTPError:
            logging.warning("Failed to fetch regions of " + name, exc_info=True)

            logging.info(f"Backup {name} table region backup ready")
            logging.info("=============================================================")

    logging.info("Done")
    sys.exit(exit_status)


@click.command(context_settings={'show_default': True})
@click.option('-s', '--target_hbase_rest_server', type=str, help="Target HBase REST server",
              required=True, metavar="HOST")
@click.option('-d', '--backup_dir', type=str, help="Backup basedir",
              default="<DEFAULT_BACKUP_FOLDER>", metavar="PATH")  # TODO: Fill in the default value!
def main(target_hbase_rest_server: str, backup_dir: str):
    hbase_url = target_hbase_rest_server
    logging.info("")
    logging.info(f"Hbase Metadata on {hbase_url} started")
    logging.info("===================================================")
    hbase_port = "<PORT_NUNMBER>" # TODO: Use your port
    url = f"http://{hbase_url}:{hbase_port}"  # TODO: Change http to https if its needed
    all_table = get_tables_name(url)
    get_all_table_backup(all_table=all_table, backup_dir=backup_dir, url=url)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main()
    logging.info("")
    logging.info("=============================================================")
    logging.info(f"Backup finished with SUCCESS")
