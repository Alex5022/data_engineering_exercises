import glob
import os
import json
import pandas as pd


def find_json_files(dirpath:str):
    """
    Yields filepaths of all json files in directory
    
    :param dirpath: Path of the directory to find json files
    """
    for file in glob.glob(f"{dirpath}/**/*.json",recursive=True):
        yield file
    

def load_file(filepath:str,load_path:str):
    """
    Load a file into a folder
    
    :param filepath: file path
    :param load_path: path of folder to load into
    """

    print(filepath)
    filename = filepath.split('/')[-1]
    print(filename)
    with open(filepath , mode = "r") as from_file:
        data = json.load(from_file)
        with open(f"{load_path}/{filename}", mode = "w") as to_file:
            json.dump(data,to_file, indent=4)
    

def load_files():
    os.makedirs("./exercise-4/downloads_json_files", exist_ok=True)
    load_path = "./exercise-4/downloads_json_files"

    for filepath in find_json_files("exercise-4/data"):
        load_file(filepath,load_path)


def flatten_json(nested_json, parent_key='', sep='_'):
        items = []
        for k, v in nested_json.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(flatten_json(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                for i, val in enumerate(v):
                    items.extend(flatten_json({f"{i}": val},new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

def data_to_csv(data:dict):
    os.makedirs("./exercise-4/csv_files",exist_ok=True)
    data = pd.DataFrame([data])
    data.to_csv(f"exercise-4/csv_files/file_{data.values[0][1]}.csv", index=False)


    


def main():
   
    for filename in os.listdir("exercise-4/downloads_json_files"):
        with open(f"exercise-4/downloads_json_files/{filename}","r") as file:
            nested_json = json.load(file)
            data_to_csv(flatten_json(nested_json))
    
    

        
        
if __name__ == "__main__":
    main()
