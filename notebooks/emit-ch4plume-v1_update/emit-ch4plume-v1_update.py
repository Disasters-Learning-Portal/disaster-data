# Import all the necessary libraries and methods

import os
import requests

import getpass

from datetime import datetime
from pathlib import Path

import pystac
import rasterio

# Import rio_stac methods
from rio_stac.stac import (
    bbox_to_geom,
    get_dataset_geom,
    get_projection_info,
    get_raster_info,
)


GHG_STAC_URL = "https://ghg.center/api/stac"

USERNAME = "<YOUR_USERNAME>"


def get_header(username, password):
    """
    Creates the authentication header to be passed to API requests
    """

    # Send the username and password to the /token endpoint to get the temporary token
    body = {
        "username": username,
        "password": password,
    }
    # request token
    response = requests.post("https://ghg.center/api/publish/token", data=body)
    if not response.ok:
        raise Exception(
            "Couldn't obtain the token. Make sure the username and password are correct."
        )
    else:
        # get token from response
        token = response.json().get("AccessToken")
        # prepare headers for requests
        headers = {"Authorization": f"Bearer {token}"}
    return headers


def get_all_items_from_cmr(collection_id):
    """
    Function that queries the cmr api to get all the data links for `collection_id`
    """
    CMR_BASE_URL = "https://cmr.earthdata.nasa.gov"
    granules_url = f"{CMR_BASE_URL}/search/granules.json?echo_collection_id={collection_id}&page_size=100"

    response = requests.get(granules_url)

    s3_links = set()
    headers = {}

    while True:
        response = requests.get(granules_url, headers=headers)
        total_granules, search_after = (
            int(response.headers["CMR-Hits"]),
            response.headers["CMR-Search-After"],
        )
        if search_after:
            headers["CMR-Search-After"] = search_after
        for entry in response.json()["feed"]["entry"]:
            s3_link = list(
                filter(
                    lambda link: link["rel"]
                    == "http://esipfed.org/ns/fedsearch/1.1/s3#",
                    entry["links"],
                )
            )[0]
            s3_links.add(s3_link["href"])

        if len(s3_links) >= total_granules:
            break
    print(f"{len(s3_links)} items discovered from cmr")
    return s3_links


def get_all_items_from_ghg(collection_id):
    """
    Function that queries the ghg center stac api to get all the data links for `collection_id`
    """
    items_url = f"{GHG_STAC_URL}/collections/{collection_id}/items"
    s3_links = set()

    while True:
        response = requests.get(items_url)

        stac = response.json()
        for item in stac["features"]:
            s3_links.add(item["assets"]["ch4-plume-emissions"]["href"])
        next = [link for link in stac["links"] if link["rel"] == "next"]
        if not next:
            break
        items_url = next[0]["href"]

    print(f"{len(s3_links)} items discovered from ghg")
    return s3_links


def ingest(dataset_definition, s3_links):
    """
    Function that takes in a dataset_definition which includes the collection
    and assets definition and s3 links to the files to be ingested; creates
    STAC item metadata for each file and ingests them to the collection using
    the publication API.

    dataset_definition: Dict; eg:

        dataset_definition = {
            "collection": ghg_emit_collection_id,
            "assets": {
                "ch4-plume-emissions": {
                    "title": "Methane Plume Complex",
                    "description": "Methane plume complexes from point source emitters",
                },
            },
        }

    s3_links: Set; eg:

        s3_links = {"s3://lp-daac-protected/EMIT/PLUME_20200413.tif", "s3://lp-daac-protected/EMIT/PLUME_20200423.tif"}
    """
    media_type = pystac.MediaType.COG

    role = ["data", "layer"]

    assets = dataset_definition.get("assets")
    collection = dataset_definition.get("collection")

    for file in s3_links:
        # Create the STAC item metadata for each file in s3_links
        filename = Path(file.split("/")[-1]).stem
        date_str = filename.split("_")[-2]
        id = filename
        assets_dict = []

        # Get datetime from filename (works only for EMIT plume complex dataset)
        single_datetime = datetime.strptime(date_str, "%Y%m%dT%H%M%S")
        for asset_key, asset_value in assets.items():
            path = file
            assets_dict.append(
                {
                    "name": asset_key,
                    "path": path,
                    "href": file,
                    "role": role,
                    "type": media_type,
                }
            )

        bboxes = []
        pystac_assets = []

        for asset in assets_dict:
            with rasterio.open(asset["path"]) as src_dst:
                # Get BBOX and Footprint
                dataset_geom = get_dataset_geom(src_dst, densify_pts=0, precision=-1)
                bboxes.append(dataset_geom["bbox"])

                proj_info = {
                    f"proj:{name}": value
                    for name, value in get_projection_info(src_dst).items()
                }
                raster_info = {"raster:bands": get_raster_info(src_dst, max_size=1024)}

                pystac_assets.append(
                    (
                        asset["name"],
                        pystac.Asset(
                            href=asset["href"] or src_dst.name,
                            media_type=media_type,
                            extra_fields={**proj_info, **raster_info},
                            roles=asset["role"],
                        ),
                    )
                )

        minx, miny, maxx, maxy = zip(*bboxes)
        bbox = [min(minx), min(miny), max(maxx), max(maxy)]

        # item
        item = pystac.Item(
            id=id,
            geometry=bbox_to_geom(bbox),
            bbox=bbox,
            collection=collection,
            stac_extensions=[],
            datetime=single_datetime,
            # **date_args,
            properties={},
        )

        # Add a link to the collection
        if collection:
            item.add_link(
                pystac.Link(
                    pystac.RelType.COLLECTION,
                    collection,
                    media_type=pystac.MediaType.JSON,
                )
            )

        for key, asset in pystac_assets:
            item.add_asset(key=key, asset=asset)

        # Publication API url/endpoint
        url = "https://ghg.center/api/publish/ingestions"

        username = USERNAME
        password = getpass.getpass()

        # Send a post request to ingest
        response = requests.post(
            url, headers=get_header(username, password), json=item.to_dict()
        )
        print(id, response.status_code)


#         with open(f"items/plumes/{id}.json", "w") as f:
#             json.dump(item.to_dict(), f)


if __name__ == "__main__":
    # Define the cmr collection id for emit plume complex
    cmr_emit_collection_id = "C2748088093-LPCLOUD"

    # Define the ghg center stac collection id for emit plume complex
    ghg_emit_collection_id = "emit-ch4plume-v1"

    # Get new links that need to be ingested into ghg center
    new_s3_links = get_all_items_from_cmr(
        cmr_emit_collection_id
    ) - get_all_items_from_ghg(ghg_emit_collection_id)

    print(f"Total {len(new_s3_links)} new items discovered")

    # Create a dataset_definition for emit in ghg
    dataset_definition = {
        "collection": ghg_emit_collection_id,
        "assets": {
            "ch4-plume-emissions": {
                "title": "Methane Plume Complex",
                "description": "Methane plume complexes from point source emitters",
            },
        },
    }

    print("Starting ingest")

    # ingest them
    ingest(dataset_definition, new_s3_links)
    print("Done")
