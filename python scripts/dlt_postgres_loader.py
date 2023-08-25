if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import subprocess

@data_loader
def dlt_bash_command(*args, **kwargs) -> None:

    def run_bash(command):

        subprocess.run(command, capture_output=True, shell=True)
        print(subprocess.check_output(command, shell=True))
    
    commands = [
        "dlt init sql_database postgres",
        "cp /home/src/default_repo/mage-docs/dlt/secrets.toml /home/src/.dlt/secrets.toml",
        "python3 /home/src/default_repo/mage-docs/dlt/postgres_loader.py"]

    for command in commands:
        run_bash(command)