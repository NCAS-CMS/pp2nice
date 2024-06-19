from pathlib import Path
import json
from minio import Minio

def get_user_config(target, location='.mc/config.json'):
    """
    Obtain credentials from user configuration file
    """
    config = Path.home()/location
    with open(config,'r') as jfile:
        jdata = json.load(jfile)
    try:
        config = jdata['aliases'][target] 
        return config
    except KeyError:
        raise ValueError(f'Minio target [{target}] not found in {jfile}')


def get_client(alias):
    """
    Get Minio client from the configuration alias, and patch the 
    client with that alias name
    """
    credentials = get_user_config(alias)
    secure = False
    if credentials['url'].startswith('https'):
        secure = True
    try:
        api = {'endpoint':'url','access_key':'accessKey','secret_key':'secretKey'}
        try:
            kw = {k:credentials[v] for k,v in api.items()}
        except KeyError:
            raise KeyError(f"Cannot find {v} in credentials supplied")
        kw['secure'] = secure
        endpoint = kw['endpoint']
        slashes = endpoint.find('//')
        if slashes > -1:
            kw['endpoint'] = endpoint[slashes+2:]
        client = Minio(**kw) 
    except:
        raise 
    # nasty monkey patch, but I want to carry this around
    client.alias_name = alias
    return client

def lswild(client, bucket, pattern='*', objects=False):
    """ 
    Do an ls on a bucket visible on the minio client which matches pattern
    This isn't quite a perfect glob! So be careful. Also, we're being cunning
    in trying to get the server to try and do some of the matching, at least
    for simple cases.
    If objects is False, return just names, oterwise return the objects
    for later processing
    """
    
    asterix = pattern.find('*')
    
    if asterix > 0:
        prefix = pattern[0:asterix]
        pattern = pattern[asterix:]
    else:
        prefix = None
    
    objects = client.list_objects(bucket, prefix=prefix)
    
    if objects:
        olist = [o for o in objects if Path(o.object_name).match(pattern)]
        return olist
    else:
        object_names = [o.object_name for o in objects]
        flist = [p for p in object_names if Path(p).match(pattern)]
        return flist
