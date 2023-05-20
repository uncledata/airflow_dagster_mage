if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import os
@data_loader
def load_seeds(*args, **kwargs):
    os.system("cd /home/src/mage-demo/dbt/dwh && dbt seed")
    # Specify your data loading logic here

