import itertools
import requests
import sys
import time
import zipfile
from minicli import cli, run

BATCH_SIZE = 100

@cli
def get(url='http://localhost:8080/geonetwork/srv',
        query=None,
        limit=0,
        bucket='default',
        magic=True,
        dryrun=False):
    """Retrieve MEF archive

    :url: Geonetwork URL, up to and including the `/srv` portion.
    :query: Additionnal query params to $url/api/q, e.g. `_source=...,isHarveste=n,type=dataset`.
    :limit: Maximum number of records to retrieve in MEF archive.
    :bucket: Bucket name for selected records.
    :magic: Retrieve record ids from magic `metadata` bucket.
    :dryrun: Dry-run mode.
    """

    api = f"{url}/api"
    session = requests.Session()

    # References:
    # - https://docs.geonetwork-opensource.org/3.12/fr/api/the-geonetwork-api/#connecting-to-the-api-with-python
    # - https://docs.geonetwork-opensource.org/4.4/fr/api/the-geonetwork-api/#connecting-to-the-api-with-python
    r = session.post(f"{api}/info?type=me")
    xsrf_token = r.cookies.get('XSRF-TOKEN')
    if not xsrf_token:
        print("Warning: Unable to find the XSRF token")

    headers = {
        'Accept': 'application/json',
        'X-XSRF-TOKEN': xsrf_token
    }

    #TODO: Support GN-4.4 ES-based $api/search/records/_search
    
    # $api/q query params
    q_params = {
        '_content_type': 'json',
        'sortBy': 'changeDate',
        'resultType': 'results',  # shortest output with uuid
        'buildSummary': 'false'
    }
    if query:
        q_params |= dict(p.split('=') for p in query.split(','))

    ids = []
    if magic:
        # Use the magic 'metadata' bucket to get ids
        r = session.get(f"{api}/q", headers=headers, params=q_params)
        # PUT with no ids => bucket is automagically filled with all matching ids
        # from the last query in session
        session.put(f"{api}/selections/metadata", headers=headers)
        r = session.get(f"{api}/selections/metadata", headers=headers)
        ids = r.json()
        if limit and len(ids) >= limit:
            ids = ids[:limit]
    else:
        # Retrieve ids the more standard way
        to = 0
        while True:
            r = session.get(f"{api}/q", headers=headers, params=q_params|{'from': to+1})
            rsp = r.json()
            to = int(rsp.get('@to'))
            newids = [record.get('uuid') for record in query_records(rsp.get('metadata', []))]
            if not newids:
                break
            ids += newids
            if limit and len(ids) >= limit:
                ids = ids[:limit]
                break
    print(f"Query returned {len(ids)} records")

    # Populate our named bucket
    for batch in itertools.batched(ids, BATCH_SIZE):
        session.put(f"{api}/selections/{bucket}", headers=headers, params={'uuid': list(batch)})

    #FIXME: Remove when sure bucket is always set
    r = session.get(f"{api}/selections/{bucket}", headers=headers)
    print(f"Safety check: bucket contains {len(r.json())} records")

    if dryrun:
        return

    print(f"Retrieving MEF archive...")
    r = session.get(f"{api}/mef.export", stream=True,
                    headers={'Accept': 'application/zip', 'X-XSRF-TOKEN': xsrf_token},
                    params = {'version': '2', 'format': 'simple', 'bucket': bucket})
    
    with open(f"export-simple-{int(time.time())}.zip", 'wb') as fd:
        for chunk in r.iter_content(chunk_size=128):
            fd.write(chunk)


@cli
def put(filename,
        url='http://localhost:8080/geonetwork/srv',
        dryrun=False):

    api = f"{url}/api"
    session = requests.Session()

    r = session.post(f"{api}/info?type=me")
    xsrf_token = r.cookies.get('XSRF-TOKEN')
    if not xsrf_token:
        # Seems this is not important...
        print("Warning: Unable to find the XSRF token")

    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/xml',
        'X-XSRF-TOKEN': xsrf_token
    }

    records = mef_records(zipfile.Path(filename))

    params = {
        'metadataType': 'METADATA',  #FIXME: set according to records' info.xml
        'uuidProcessing': 'OVERWRITE',
        # 'assignToCatalog': 'true'  # only MEF
    }

    print("Updating records...")
    i = 0
    for record in records:
        print(record['id'])
        data = record['path'].read_text()
        r = session.put(f"{api}/records", auth=('admin', 'admin'),
                        headers=headers, params=params, data=data)
        r.raise_for_status()
        i += 1
    print(f"Updated {i} records")


def query_records(metadata):
    # Geonetwork $api/records/q json 'metadata' list is broken
    for m in metadata:
        if isinstance(m, list):
            yield m[0]
        else:
            yield m

def mef_records(path):
    for p in path.iterdir():
        if not p.is_dir():
            continue
        md = p / 'metadata' / 'metadata.xml'
        if not md.exists():
            continue
        yield {'id': p.name, 'path': md}
            

if __name__ == '__main__':
    run()
