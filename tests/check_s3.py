from upload import move_to_s3
import sys

target, bucket, filename = tuple(sys.argv[1:4])

user_metadata={'test_upload':'bryan is playing'}

move_to_s3(filename, target, bucket, user_metadata=user_metadata)



