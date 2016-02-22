# -*- coding: utf-8 -*-

"""
Attempt to import SQl services, warn gracefully if unsuccessful.
Originally written by Martin for his BREAST program.
Updated for use outside of BREAST by Maggie Kusano, February 3, 2016.
"""

try:
    from ._sql_run import *
except Exception, e:
    print e
    print "No SQL services"   
