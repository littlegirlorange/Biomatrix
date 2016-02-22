# -*- coding: utf-8 -*-
"""
Gets all user preferences from the .ini file, or generates defaults if the file doesn't exist.
Originally written by Martin for his BREAST program.
Updated for use outside of BREAST by Maggie Kusano, February 3, 2016.
"""
import os
here = os.path.dirname(os.path.abspath(__file__)) + os.sep

def load_pars(filename):
    pars = {}
    for line in open(filename):
        if '=' in line:
            try:
                newdict = eval("dict(%s)"%line)
                pars.update(newdict)
            except:
                pass
    return pars

default_pars = load_pars(here+'defaultpars.ini')
try:
    pars = default_pars.copy()
    pars.update(load_pars(here+'pars.ini'))
except:
    pars = default_pars

def save_pars():
    fred = open(here+'pars.ini', 'w')
    for line in open(here+'defaultpars.ini'):
        l2 = line.split('#')[0]
        if '=' in l2:
            parname = l2[:l2.find('=')].rstrip().lstrip()
            outline = "%s = %s \n"%(parname, repr(pars[parname]))
            fred.write(outline)
        else:
            fred.write(line)
    fred.close()
