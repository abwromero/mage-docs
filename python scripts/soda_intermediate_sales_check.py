if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import subprocess

@custom
def soda_intermediate_sales_table_check(data, *args, **kwargs):

    def soda_intermediate_sales_values_check(command):

        subprocess.run(command, capture_output=True, shell=True)
        print(subprocess.check_output(command, shell=True))

    command = "soda scan -d car_sales_database -c /home/src/default_repo/mage-docs/soda/configuration.yml /home/src/default_repo/mage-docs/soda/check_intermediate_sales_condensed.yml"

    soda_intermediate_sales_values_check(command)