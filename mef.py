import itertools
import re
import requests
import sys
import time
import zipfile
from minicli import cli, run

BUCKET_NAME = 'mef'
BUCKET_BATCH_SIZE = 100

@cli('format', choices=['simple', 'partial', 'full'])
def get(url='http://localhost:8080/geonetwork/srv',
        query=None,
        format='simple',
        limit=0,
        batch=500,
        magic=True,
        dryrun=False):
    """Retrieve MEF archive

    :url: Geonetwork URL, up to and including the `/srv` portion.
    :query: Additionnal query params to $url/api/q, e.g. `_source=...,isHarvested=n,type=dataset`.
    :format: MEF format.
    :limit: Maximum number of records to retrieve in MEF archive.
    :batch: Retrieve results in batches (multiple MEF files).
    :magic: Retrieve record ids from magic `metadata` bucket.
    :dryrun: Dry-run mode.
    """

    api = f"{url}/api"
    session = requests.Session()

    # References:
    # - https://docs.geonetwork-opensource.org/3.12/fr/api/the-geonetwork-api/#connecting-to-the-api-with-python
    # - https://docs.geonetwork-opensource.org/4.4/fr/api/the-geonetwork-api/#connecting-to-the-api-with-python
    r = session.post(f"{url}/eng/info?type=me")
    # don't abort on error here, it's expected...
    _ = get_cookie(r.cookies, 'JSESSIONID')
    xsrf_token = get_cookie(r.cookies, 'XSRF-TOKEN')

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
        abort_on_error(r)
        # PUT with no ids => bucket is automagically filled with all matching ids
        # from the last query in session
        r = session.put(f"{api}/selections/metadata", headers=headers)
        abort_on_error(r)
        r = session.get(f"{api}/selections/metadata", headers=headers)
        abort_on_error(r)
        ids = r.json()
        if limit and len(ids) >= limit:
            ids = ids[:limit]
    else:
        # Retrieve ids the more standard way
        to = 0
        while True:
            r = session.get(f"{api}/q", headers=headers, params=q_params|{'from': to+1})
            abort_on_error(r)
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

    mef_batches = [ids] if batch <= 0 else list(itertools.batched(ids, batch))
    n = len(mef_batches)
    for i, mef_batch in enumerate(mef_batches, start=1):
        # Populate our named bucket iteratively to avoid 'request too long'
        for b in itertools.batched(mef_batch, BUCKET_BATCH_SIZE):
            r = session.put(f"{api}/selections/{BUCKET_NAME}", headers=headers, params={'uuid': list(b)})
            abort_on_error(r)

        r = session.get(f"{api}/selections/{BUCKET_NAME}", headers=headers)
        abort_on_error(r)

        print(f"[{i}/{n}] Retrieving {format} MEF archive ({len(r.json())} records)...")
        timestamp = int(time.time())
        part = 'all' if n == 1 else f"{i:02}"
        filename = f"export-{format}-{timestamp}-{part}.zip"

        if dryrun:
            print(f"Would write {filename}")
        else:
            r = session.get(f"{api}/mef.export", stream=True,
                            headers={'Accept': 'application/zip', 'X-XSRF-TOKEN': xsrf_token},
                            params={'version': '2', 'format': format, 'bucket': BUCKET_NAME})
            abort_on_error(r)
            with open(filename, 'wb') as fd:
                for chunk in r.iter_content(chunk_size=128):
                    fd.write(chunk)
            print(f"Wrote {filename}")

        r = session.delete(f"{api}/selections/{BUCKET_NAME}", headers=headers)
        abort_on_error(r)


@cli('mode', choices=['record', 'records', 'mef'])
def put(filename,
        mode='record',
        url='http://localhost:8080/geonetwork/srv'):
    """Import MEF archive

    :filename: MEF archive filename.
    :mode: Import mode.
    :url: Geonetwork URL, up to and including the `/srv` portion.
    """

    api = f"{url}/api"
    session = requests.Session()

    r = session.post(f"{api}/info?type=me")
    abort_on_error(r)
    xsrf_token = r.cookies.get('XSRF-TOKEN')
    if not xsrf_token:
        # Seems this is not important...
        print("Warning: Unable to find the XSRF token")

    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/xml',
        'X-XSRF-TOKEN': xsrf_token
    }

    params = {
        'metadataType': 'METADATA',  #FIXME: set according to records' info.xml
        'uuidProcessing': 'OVERWRITE',
        # 'assignToCatalog': 'true'  # only MEF
    }

    print("Updating records...")
    if mode == 'mef':
        #FIXME: Can't get it to work:
        #  Cannot build ServiceRequest
        #  Cause : Error on line 1: Content is not allowed in prolog.
        #  Error : org.jdom.input.JDOMParseException
        r = session.post(f"{api}/mef.import", auth=('admin', 'admin'),
                         headers=headers, params=params|{'file_type': 'mef'},
                         files={'mefFile': (filename, open(filename, 'rb'))})
        abort_on_error(r)
    elif mode == 'records':
        #FIXME: Can't get it to work:
        #  {"message":"IllegalArgumentException","code":"unsatisfied_request_parameter","description":"A file MUST be provided."}
        form_data = (
            {k: (None, v) for k,v in params.items()}
            | {'file': (filename, open(filename, 'rb'), 'application/zip')}
        )
        r = session.post(f"{api}/records", auth=('admin', 'admin'),
                         headers=headers, files=form_data)
        abort_on_error(r)
    elif mode == 'record':
        recs = mef_records(zipfile.Path(filename))
        i = 0
        for rec in recs:
            print(rec['id'])
            data = rec['path'].read_text()
            r = session.put(f"{api}/records", auth=('admin', 'admin'),
                            headers=headers, params=params, data=data)
            abort_on_error(r)
            i += 1
        print(f"Updated {i} records")


def get_cookie(jar, name):
    val = jar.get(name)
    if val:
        print(f"{name}={val}")
    else:
        print(f"Warning: Unable to find {name}")
    return val

def abort_on_error(r):
    try:
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(e)
        try:
            # poor man's html stipping
            m = re.search('<body>.*</body>', r.text, re.IGNORECASE | re.DOTALL)
            print('---')
            if m:
                print(re.sub('<[^<]+?>', ' ', m[0]))
            else:
                try:
                    print(r.json())
                except requests.exceptions.JSONDecodeError:
                    print(r.text)
        except:
            pass
        sys.exit(1)

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
