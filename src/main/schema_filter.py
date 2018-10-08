import sys
import os
import botocore
import boto3
import lazyreader

s3_bucket = "scdi-test"

def check_header(obj):
    """
    Reads the header from the file and checks for eight field in the header
    Input : filename
    Output : returns True if
    """
    print "in check header"
    uses_gps_coord = True
    print obj
    for line in lazyreader.lazyread(obj.get()['Body'], delimiter='\n'):
        header = line.strip()
        print header
        if header:
            fields = header.split(",")
            if (len(fields) > 8):
                if fields[7] == 'PULocationID':
                    return False
                else:
                    return True
        return None


def get_s3files():
    file_list = []
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(s3_bucket)

    for obj in bucket.objects.all():
        file_name = obj.key
        if not file_name.startswith('yellow'):
            continue
        uses_gps_coord = check_header(obj)
        msg = "Checking schema for file {0} : ".format(file_name)
        if uses_gps_coord is None:
            print msg + "Undefined"
            continue

        elif uses_gps_coord:
            print msg + "GPS"
            os.system('./run.sh GPS')
        else:
            print msg + "TAXIZONE"
            os.system('./run.sh TAXIZONE')


if (__name__=='__main__'):
    print "in main"
    try:
        get_s3files()
        print "Schema Filtering process complete"
    except Exception as e:
        print "Schema Filtering process failed", e

