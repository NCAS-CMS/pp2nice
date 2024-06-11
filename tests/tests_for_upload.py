import tempfile
import numpy as np
from pp2nice import move_to_s3, get_user_config, minio_upload
import os
from pathlib import Path


# for these tests to pass, there must be an "hpos" entry in minio config file for the 
# user running the tests which is for S3, and there must be a bucket called "bnl" at
# that endpoint. These can be changed for other users.
# runnig 
TARGET = 'hpos'
BUCKET = 'bnl'


def test_move_fail(target=TARGET, bucket=BUCKET):
    """
    Test that move and delete does what you think it will do
    """
    size = 1024
    #with tempfile.NamedTemporaryFile("w",delete_on_close=False) as fp:
    with tempfile.NamedTemporaryFile("w",delete=False) as fp:
        data = np.ones(size)
        data.tofile(fp)
        fname = fp.name
        move_to_s3(fname, target, bucket, testfail=True)
        assert os.path.exists(fname)
        fp.file.close()
    

def test_move_succeed(target=TARGET, bucket=BUCKET):
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
       
def test_upload(target=TARGET, bucket=BUCKET):
    """
    Copies this file to the bucket at target. Used for testing
    """
    mypath = Path(__file__)
    credentials = get_user_config(target)
    secure = False
    if credentials['url'].startswith('https'):
        secure = True
    minio_upload(mypath, credentials, bucket,
        secure=secure,
        object_name = 'test_file_delete_at_whenever.py'
        )

