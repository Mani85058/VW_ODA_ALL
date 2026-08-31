import os

def get(name: str):
    """
    Get method performs the collection of variables added as arguments
    """

    variable_name = name
    if variable_name == 'tab_yaml_path':
        variable_name ='TAB_YAML_PATH'

    if not variable_name in os.environ:
        # print ('env.get: not found ' + variable_name)
        raise KeyError('no variable ' + variable_name)

    return os.environ[variable_name]

