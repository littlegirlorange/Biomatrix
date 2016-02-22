# -*- coding: utf-8 -*-
"""
Process queries to BioMatrix database.
Originally written by Martin for his BREAST program.
Updated for use outside of BREAST by Maggie Kusano, February 3, 2016.

Specific syntax:

- a class name by itself (Patient, Series): all entries of that table

- an expression on a single class field (Patient.gender_int=="None")

- comma-separated list of the above, which will be AND-ed together.

Exporting from crosstab will produce the latter of the three options

TODO: join tables in the case that the comma-separated operators refer to
different entities.
"""

from fetch_data.sql_run import *
sessions=[]

def process_query(query, session=None):
    if not session:
        session = Session()
        sessions.append(s)
    if not('.' in query):
        return s.query(eval(query)).all()
    bits = query.split(',')
    args = []
    for bit in bits:
        dot = bit.find('.')
        entity = eval(bit[:dot])
        args.append(eval(bit))
    return session.query(entity).filter(*args).all()
