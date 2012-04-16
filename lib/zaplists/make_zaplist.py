import numpy as N
import pylab as PL
from scipy import signal
import database

use_db        = False
hi_threshold  = 200 # Anything with fmag > than this will get zapped
std_threshold = 6   # Anything > than this many standard deviations gets zapped
win_size      = 500 # Size of window to use for histogram stats


for backend in ["wapp","Mock"]:
    birdfile = open("PALFA_commondb_%s.birds"%backend, "w")
    birdfile.write("#      Freq   Width  harm  grow?  bary?\n")
    zapfile  = open("PALFA_commondb_%s.zaplist"%backend, "w")
    zapfile.write("# This file created by R. Lynch through inspection of the PALFA common-db\n")
    zapfile.write("# Lines beginning with \'#\' are comments\n")
    zapfile.write("# Lines beginning with \'B\' are barycentric freqs\n")
    zapfile.write("#                 Freq                 Width\n")
    zapfile.write("# --------------------  --------------------\n")
    
    if use_db:
        db = database.Database('common')
        query = "SELECT c.frequency FROM pdm_candidates AS c " \
            "LEFT JOIN headers AS h ON h.header_id=c.header_id " \
            "WHERE h.source_name LIKE 'G%%' AND h.obsType='%s'"%backend
        db.execute(query)
        freqs = N.squeeze(N.array(db.cursor.fetchall()))
    else:
        freqs = N.fromfile("PALFA_commondb_%s_allfreqs.nda"%backend)
        
    df    = 1/300.0   # Fourier frequency bin width (from integration time)
    fmax  = 1/0.0005  # The minimum candidate period to fold is 0.5 ms
    # Number of bins to use in histogram (bins will be 2*df wide)
    nbins = fmax/df   
    
    fmags,fbins = N.histogram(freqs, nbins, range=(0,fmax))
    forig  = fmags.copy()
    fbins  = fbins[:-1] + N.diff(fbins)/2.0
    
    birds     = []
    still_zap = True
    # First zap any thing with fmag > hi_threshold
    count       = 0
    for ii,(fmag,fbin) in enumerate(zip(fmags,fbins)):
        if fmag > hi_threshold:
            birds.append(fbin)
            count    += 1
            fmags[ii] = 0
            
    npasses = 0
    while still_zap:
        npasses += 1
        print "Making pass #%i through histogram"%npasses
        run_count = 0
        for ii,(fbin,fmag) in enumerate(zip(fbins,fmags)):
            if ii < win_size/2: wmin = 0
            else: wmin = ii - win_size/2
            if ii >= len(fbins) - win_size/2: wmax = len(fbins)
            else: wmax = ii + win_size/2
            window = N.concatenate((fmags[wmin:ii],fmags[ii+1:wmax]))
            
            if (fmag - N.median(window))/N.std(window) > std_threshold:
                birds.append(fbin)
                count     += 1
                run_count += 1
                fmags[ii]  = 0

        if run_count == 0: still_zap = False
        
    for bird in birds:
        birdfile.write("%11.5f %7.5f     1      0      1\n"%(bird,df))
        zapfile.write("B %20.10f  %20.10f\n"%(bird,df))
        #PL.axvline(bird, color="red")
    
    birdfile.close()
    zapfile.close()
    
    print "%i frequencies zapped (%.2f%%) in %s data"% \
          (count, 100*float(count)/float(len(fmags)), backend)
    ax1 = PL.subplot(211)
    ax1.plot(fbins,forig)
    ax2 = PL.subplot(212)
    ax2.plot(fbins,fmags)
    PL.show()
