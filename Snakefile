from os import path

PROJECT_NAME = "tstore"
CODE_DIR = "tstore"
PYTHON_VERSION = "3.10"

NOTEBOOKS_DIR = "notebooks"
NOTEBOOKS_OUTPUT_DIR = path.join(NOTEBOOKS_DIR, "output")

DATA_DIR = "data"
DATA_RAW_DIR = path.join(DATA_DIR, "raw")
DATA_INTERIM_DIR = path.join(DATA_DIR, "interim")
DATA_PROCESSED_DIR = path.join(DATA_DIR, "processed")


# 0. conda/mamba environment -----------------------------------------------------------
rule create_environment:
    shell:
        "mamba env create -f environment.yml"


rule register_ipykernel:
    shell:
        f"python -m ipykernel install --user --name {PROJECT_NAME} --display-name"
        f' "Python ({PROJECT_NAME})"'


# 1. get data --------------------------------------------------------------------------
REGION = "Canton de Vaud"
# 1.1 stations -------------------------------------------------------------------------
STATIONS_NOTEBOOK_FILENAME = "01-get-stations-data.ipynb"
STATIONS_DATA_DIR = path.join(DATA_PROCESSED_DIR, "stations")


rule get_stations_data:
    input:
        notebook=path.join(NOTEBOOKS_DIR, STATIONS_NOTEBOOK_FILENAME),
    output:
        notebook=path.join(NOTEBOOKS_OUTPUT_DIR, STATIONS_NOTEBOOK_FILENAME),
        stations=path.join(STATIONS_DATA_DIR, "stations.gpkg"),
    shell:
        "papermill {input.notebook} {output.notebook} -p region '{REGION}'"
        " -p dst_stations_filepath {output.stations}"


# 1.2 variables ------------------------------------------------------------------------
VARIABLES = ["temperature", "water_vapour"]
VARIABLES_NOTEBOOK_FILENAME = "02-get-variables-data.ipynb"
VARIABLES_DATA_DIR = path.join(DATA_PROCESSED_DIR, "variables")


rule get_variable_data:
    input:
        notebook=path.join(NOTEBOOKS_DIR, VARIABLES_NOTEBOOK_FILENAME),
    output:
        notebook=path.join(NOTEBOOKS_OUTPUT_DIR, "02-get-{variable}-data.ipynb"),
        variable=path.join(VARIABLES_DATA_DIR, "{variable}.gpkg"),
    shell:
        "papermill {input.notebook} {output.notebook} -p region '{REGION}'"
        " -p variable {wildcards.variable} -p dst_variable_filepath {output.variable}"


# rule get_variables_data:
#     input:
#         expand(
#             rules.get_variable_data.output.variable,
#             variable=VARIABLES
#         )


rule all:
    input:
        stations=rules.get_stations_data.output.stations,
        variables=expand(rules.get_variable_data.output.variable, variable=VARIABLES),
