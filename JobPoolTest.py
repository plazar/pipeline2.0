from job import *

files_in = ['4bit-p2030.20100810.B2020+28.b0s0g0.00100.fits',
            '4bit-p2030.20100810.B2020+28.b0s8g0.00100.fits',
            '4bit-p2030.20100810.B2020+28.b0s2g0.00103.fits',
            '4bit-p2030.20100810.B2020+28.b0s7g0.00103.fits',
            '4bit-p2030.20100810.B2020+28.b0s1g0.00103.fits',
            '4bit-p2030.20100810.B2020+28.b0s0g0.00104.fits',
            '4bit-p2030.20100810.B2020+28.b0s4g0.00105.fits',
            'p2030.20100810.B2020+28.b0s4g0.00105.fits',
            ]
jpool= JobPool()
fn_groups = jpool.group_files(files_in)

i = 0
for fg in fn_groups:
    print "Group #"+ str(i)
    if isinstance(fg,list):
        for fn in fg:
            print fn
    else:
        print fg
    print "-------"
    i+=1
#print jpool.datafiles
#print jpool.merged_dict