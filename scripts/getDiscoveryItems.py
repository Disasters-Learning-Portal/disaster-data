from utils import  getDiscoveryItemPath, getDataSetPath, readJSON, writeJSON

def getDiscoveryItemFromDataset(dataset):
    """
    Extracts the first discovery item from the dataset, excluding the 'discovery' key.
    Returns the discovery item or None if it doesn't exist.
    """
    key_to_exclude = "discovery"
    items_from_dataset = dataset.get('discovery_items', [])
    if items_from_dataset:
        first_item = items_from_dataset[0]
        filtered_data = {k: v for k, v in first_item.items() if k != key_to_exclude}
        return filtered_data
    return None

def createDiscoveryItem(name):
    """Creates a discovery item JSON from the dataset and writes it to a file."""
    discovery_item_path = getDiscoveryItemPath(name)
    dataset_path = getDataSetPath(name)
    
    dataset = readJSON(dataset_path)
    if dataset is None:
        print("Error: Missing dataset.")
        return
    
    if 'collection' not in dataset:
        print("Error: Invalid dataset.")
        return
    
    discovery_item = getDiscoveryItemFromDataset(dataset)
    if discovery_item is not None:
        writeJSON(discovery_item, discovery_item_path)
    else:
        print("Error: No discovery item found in the dataset.")
