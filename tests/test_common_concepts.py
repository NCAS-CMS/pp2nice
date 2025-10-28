from pp2nice.common_concept import CommonConcepts
import cf

WD = '/Users/bnl28/data/'
F = 'ch330a.p919810219.pp'


def test_consistency():
    """ 
    Verify that the ncas common concept database does not overlap
    with the CMIP one.
    """
    c = CommonConcepts()
    names=[]
    for k,v in c.db['index'].items():
        for kk,vv in v.items():
            for n in vv:
                names.append(n)
    for n,v in c.ncasdb.items():
        if v in names:
            print('Duplicate for ',n,v)


def test_common_concept():

    fn = WD+F
    ff = cf.read(fn)
    c = CommonConcepts()
    for f in ff:

        r = c.identify(f)
        print(f.standard_name, r)

    