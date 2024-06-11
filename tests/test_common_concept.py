import cf
from pp2nice import CommonConcepts, verify_consistency


WORKING_DIR ='/home/users/lawrence/hiresgw/hrcm/hrcm/n1280run/u-ch330/1hrly/'
TEST_PP_FILE =  'ch330a.p919810219.pp'


def test_commonconcept():
    wd = WORKING_DIR
    f = TEST_PP_FILE
    fn = wd+f
    ff = cf.read(fn)
    c = CommonConcepts()
    for f in ff:
        r = c.identify(f)
        print(f.standard_name, r)


def test_consistency():
    verify_consistency()