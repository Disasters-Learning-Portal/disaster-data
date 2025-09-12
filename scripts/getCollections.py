from utils import getCollectionPath,  getDataSetPath, readJSON, writeJSON


def getCollectionFromDataset(dataset):
    """
    Extracts the relevant collection information from the dataset.
    Returns a collection item dictionary based on the dataset structure.
    """
    collectionItem = {
        "id": dataset.get('collection', ""),
        "type": "Collection",
        "links": dataset.get('links', []),
        "keywords": dataset.get('keywords', []),
        "providers": dataset.get('providers', []),
        "extent": {
            "spatial": {
                "bbox": [[
                    dataset.get('spatial_extent', {}).get('xmin', 0),
                    dataset.get('spatial_extent', {}).get('ymin', 0),
                    dataset.get('spatial_extent', {}).get('xmax', 0),
                    dataset.get('spatial_extent', {}).get('ymax', 0)
                ]]
            },
            "temporal": {
                "interval": [[
                    dataset.get('temporal_extent', {}).get('startdate', None),
                    dataset.get('temporal_extent', {}).get('enddate', None)
                ]]
            }
        },
        "stac_version": dataset.get('stac_version', ""),
        "stac_extensions": dataset.get('stac_extensions', [""]),
        "title": dataset.get('title', ""),
        "description": dataset.get('description', ""),
        "license": dataset.get('license', ""),
        "renders": dataset.get('renders', {}),
        "item_assets": dataset.get('item_assets', {}),
        "assets": dataset.get('assets', {}),
        "dashboard:is_periodic": dataset.get('is_periodic', ""),
        "dashboard:time_density": dataset.get('time_density', "")
    }
    return collectionItem

def createCollectionItem(name):
    """Creates a collection item JSON from the dataset and writes it to a file."""
    dataSetPath = getDataSetPath(name)
    collectionPath = getCollectionPath(name)

    dataset = readJSON(dataSetPath)

    if dataset is None:
        print("Error: Missing dataset.")
        return
    
    if 'collection' not in dataset:
        print("Error: Invalid dataset.")
        return
    
    collectionItem = getCollectionFromDataset(dataset)
    if collectionItem is not None:
        writeJSON(collectionItem, collectionPath)
    else:
        print("Error: No Collection found in the dataset.")

        


