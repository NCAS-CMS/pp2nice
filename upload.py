from pathlib import Path
from minio import Minio
from s3v.s3core import get_user_config, sanitised_metadata
import os
import tempfile
import numpy as np
import pytest
import time



def minio_upload(file_path, credentials, bucket, metadata=None, secure=True, object_name=None, verify=False):

    """ 
    Upload the POSIX file at path to the bucket using the minio 
    credentials configuration for a specific alias 
    found in the users home .mc/config.json.
    The file will be uploaded into the specficic bucket, and 
    if object_name is provided, named accordingly, otherwise
    the filename will be used (sans path).
    If you know that the target is using http rather than https, pass
    secure = False.
    If you request verification, in principle we can test if the
    uploaded file is the same as on disk, but at the momement we 
    can't do that properly.
    We can upload user metadata key value pairs if provided as dictionary
    """
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
    if object_name is None:
        object_name = Path(file_path).name
    
    #make the bucket if it does not exist
    try:
        found = client.bucket_exists(bucket)
    except:
        print('** CHECK ENDPOINT ADDRESS and SECURE OPTION')
        raise
    if not found:
        ok = client.make_bucket(bucket)
        print('Created bucket', bucket)

    #upload
    try:
        result = client.fput_object(bucket, object_name, file_path, metadata=metadata)
        etag = result.etag
        if do_verify:
            size = Path(file_path).stat().st_size
            do_verify(size, etag, client, bucket, object_name, metadata)
    except:
        raise


def move_to_s3(file_path, target, bucket, user_metadata=None, testfail=False, logging=True):
    """
    Move <file_path> to <bucket> at the minio <target> (from
    your credential file). NOTE THAT THE FILE AT FILE_PATH
    IS REMOVED FROM DISK AFTER SUCCESSFUL COPY TO S3.

    <testfail> is used for testing only and should not be
    used in production.
    """
    credentials = get_user_config(target)
    secure = False
    if credentials['url'].startswith('https'):
        secure = True
    try:
        e1 = time.time()
        minio_upload(file_path, credentials, bucket, secure=secure, metadata=user_metadata)
        if testfail:
            raise RuntimeError('Testing failure required')
        e2 = time.time() - e1
        if logging:
            print(f'Upload of {file_path} took {e2:.1f}s')
        os.remove(file_path)
    except:
        raise RuntimeError('Unexpected issue with S3 copy. POSIX file not deleted')


def do_verify(file_size, etag, client, bucket, object_name, metadata):
    """ 
    Ideally we verify the file is correct by first checkging the size in bytes
    and then we can try and work out whether or not the etag is correct. But the
    etag is complicated, it's not necessarily the MD5 checksum, so a the moment
    the second step is not done. A warning is raised.
    """
    result = client.stat_object(bucket, object_name)
    object_size = result.size
    if object_size != file_size:
        raise RuntimeError(f'Object size ({object_size}) does not match file size ({file_size})')
    print(f'Warning - Cannot verify using checksums - but at least file sizes do match for: {object_name}!')
    # see this useful stackoverflow: 
    # https://stackoverflow.com/questions/62555047/how-is-the-minio-etag-generated
    if metadata is not None:
        ometa = {k[11:]:v for k,v in result.metadata.items() if k.startswith('x-amz-meta')}
        ometa = sanitised_metadata(sorted(ometa))
        umeta = sanitised_metadata(sorted(metadata))
        if ometa != umeta:
            raise RuntimeError('Metadata not preserved - u{umeta} - o {ometa}')

def test_move_fail(target="hpos", bucket="bnl", secure=False):
    """
    Test that move and delete does what you think it will do
    """
    size = 1024
    #with tempfile.NamedTemporaryFile("w",delete_on_close=False) as fp:
    with tempfile.NamedTemporaryFile("w",delete=False) as fp:
        data = np.ones(size)
        data.tofile(fp)
        fname = fp.name
        fp.file.close()
        with pytest.raises(RuntimeError):
            move_to_s3(fname, target, bucket, testfail=True)
        assert os.path.exists(fname)
         

def test_move_succeed(target="hpos", bucket="bnl", secure=False):
    """
    Test that move and delete does what you think it will do
    """
    size = 1024
    #with tempfile.NamedTemporaryFile("w",delete_on_close=False) as fp:
    with tempfile.NamedTemporaryFile("w",delete=False) as fp:
        data = np.ones(size)
        data.tofile(fp)
        fname = fp.name
        move_to_s3(fname, target, bucket, testfail=False)
        assert not os.path.exists(fname)
        fp.file.close()
       
def testme(target="hpos", bucket="bnl", secure=True):
    """
    Copies this file to the bucket at target. Used for testing
    """
    mypath = Path(__file__)
    credentials = get_user_config(target)
    minio_upload(mypath, credentials, bucket,
        secure=secure,
        object_name = 'test_file_delete_at_whenever.py'
        )
    
def test_move_metadata(target='hpos', bucket='bnl',secure=False):
    size = 1024
    with tempfile.NamedTemporaryFile("w",delete=False) as fp:
        data = np.ones(size)
        data.tofile(fp)
        fname = fp.name
        move_to_s3(fname, target, bucket, user_metadata={'meta':'test','emeta':'test2'}, testfail=False)
        assert not os.path.exists(fname)
        fp.file.close()    


if __name__=="__main__":

    test_move_succeed()
    test_move_fail()
    test_move_metadata()

