import os
from utils import getCollectionPath, getDiscoveryItemPath, getDataSetPath, readJSON, writeJSON

def getDataset(collectionData, discoveryItemData):
    """
    Generates a dataset object using the collection and discovery item data.
    Assumes the presence of certain keys in the collection and discovery data.
    """
    return {
        "collection": collectionData.get('id'),
        "data_type": "cog",
        "spatial_extent": {
            "xmin": collectionData.get('extent', {}).get('spatial', {}).get('bbox', [[0, 0, 0, 0]])[0][0],
            "ymin": collectionData.get('extent', {}).get('spatial', {}).get('bbox', [[0, 0, 0, 0]])[0][1],
            "xmax": collectionData.get('extent', {}).get('spatial', {}).get('bbox', [[0, 0, 0, 0]])[0][2],
            "ymax": collectionData.get('extent', {}).get('spatial', {}).get('bbox', [[0, 0, 0, 0]])[0][3]
        },
        "temporal_extent": {
            "startdate": collectionData.get('extent', {}).get('temporal', {}).get('interval', [[None, None]])[0][0],
            "enddate": collectionData.get('extent', {}).get('temporal', {}).get('interval', [[None, None]])[0][1]
        },
        "stac_version": collectionData.get('stac_version',""),
        "stac_extensions": collectionData.get('stac_extensions',[]),
        "title": collectionData.get('title',""),
        "description": collectionData.get('description',""),
        "discovery_items": [
            {
                "discovery": 's3' , # Default value for discovery
                **(discoveryItemData if discoveryItemData is not None else {}),
                
            }
        ],
        "is_periodic": collectionData.get('dashboard:is_periodic',""), 
        "license": collectionData.get('license',""),
        "sample_files": collectionData.get('sample_files',['']),
        "providers": collectionData.get('providers',[]), 
        "renders": collectionData.get('renders',{}),
        "item_assets": collectionData.get('item_assets',{}),
        "assets":  collectionData.get('assets' ,{}), 
        "time_density": collectionData.get('dashboard:time_density',"")
    }

def createDataset(name):
    """
    Creates a dataset by combining collection data and discovery item data.
    Writes the dataset to a file if the collection data is valid.
    """
    collectionPath = getCollectionPath(name)
    discoveryItemPath = getDiscoveryItemPath(name)
    dataSetPath = getDataSetPath(name)

    collectionData = readJSON(collectionPath)
    discoveryItemData = readJSON(discoveryItemPath)

    # Check if collection data exists and is valid
    if collectionData is None:
        print("Error: Missing Collection Item.")
        return
    
    if 'id' not in collectionData:
        print("Error: Invalid Collection Item.")
        return

    dataset = getDataset(collectionData, discoveryItemData)
    if dataset:
        writeJSON(dataset, dataSetPath)


def createDatasetsForAllCollections():
    folderPath = os.path.join("..", "ingestion-data", "collections")   
    print(folderPath)
    for file in os.listdir(folderPath):
        if file.endswith('.json'): # Only process JSON files
            filename = os.path.splitext(file)[0]
            createDataset(filename)

# createDatasetsForAllCollections()
