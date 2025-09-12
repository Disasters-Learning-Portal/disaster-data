import json
import os

def getCollectionPath(name):
    return os.path.join("..", "ingestion-data", "collections", f"{name}.json")

def getDiscoveryItemPath(name):
    return os.path.join("..", "ingestion-data", "discovery-items", f"{name}-items.json")

def getDataSetPath(name):
    return os.path.join("..", "ingestion-data", "datasets", f"{name}.json")

def writeJSON(data, filePath):
    try:
        with open(filePath, 'w') as json_file:
            json.dump(data, json_file, indent=4)
            json_file.write('\n')
        print(f"Data successfully written to {filePath}.")
    except (FileNotFoundError, PermissionError, TypeError) as e:
        print(f"Error: {str(e)}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def readJSON(filePath):
    try:
        with open(filePath, "r") as file:
            return json.load(file)
    except (FileNotFoundError, json.JSONDecodeError, PermissionError) as e:
        print(f"Error: {str(e)}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")