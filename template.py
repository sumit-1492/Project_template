import os
from pathlib import Path ## where we want to create different folders
import logging

## list of files /components /logger
while True:
    project_name = input("Enter your project name: ")
    if project_name != '':
        break

logging.info(f"creating project name: {project_name}")
list_of_files = [
    ".github/workflows/.gitignore",
    ".github/workflows/main.yaml",
    f"{project_name}/__init__.py" ,   ## first it will ask project name..it will create _init__.py file
    f"{project_name}/components/__init__.py" ,
    f"{project_name}/config/__init__.py" ,
    f"{project_name}/constant/__init__.py" ,
    f"{project_name}/entity/__init__.py" ,
    f"{project_name}/exception/__init__.py" ,
    f"{project_name}/logger/__init__.py" ,
    f"{project_name}/pipeline/__init__.py" ,
    f"{project_name}/utils/__init__.py" ,
    f"config/config.yaml",
    "requirements.txt",
    "setup.py",
    "main.py",
    "schema.yaml"
]

for filepath in list_of_files:
    filepath = Path(filepath)
    filedir, filename = os.path.split(filepath)

    if filedir != "":
        os.makedirs(filedir,exist_ok=True)
        logging.info(f"creating new directory at : {filedir} for filr:{filename}")

    if (not os.path.exists(filepath)) or (os.path.getsize(filepath)==0):
        with open(filepath,"w") as f:
            pass
            logging.info(f"creating new file at : {filename} for filr:{filepath}")
    else:
        logging.info(f"File is already present as : {filepath}")