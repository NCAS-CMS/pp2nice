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
        move_to_s3(fname, target, bucket, user_metadata={'META':'test','emeta':'test2'}, testfail=False)
        assert not os.path.exists(fname)
        fp.file.close()    


if __name__=="__main__":

    test_move_succeed()
    test_move_fail()
    test_move_metadata()

