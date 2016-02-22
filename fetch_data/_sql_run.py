# -*- coding: utf-8 -*-
"""
BIOMATRIX table mapping schema, using SQLAlchemy ORM.
Originally written by Martin for his BREAST program.
Updated for use outside of BREAST by Maggie Kusano, February 3, 2016.
"""

import datetime
import numpy as np
import sqlalchemy as al
import sqlalchemy.orm
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
def connect(driver, uname, pword, ip , dbase):
    """Re-establish connection using supplied information (all as strings)."""
    global Session,s,engine
    con = "%s://%s:%s@%s/%s?connect_timeout=1"%(driver, uname, pword, ip, dbase)
    engine = al.create_engine(con)
    engine.connect()
    Base.metadata.bind = engine
    Session = al.orm.sessionmaker(bind=engine)
    s = Session()

from loadsavepars import pars
connect(pars['driver'], pars['user'], pars['pword'], pars['host'], pars['base'])

class Patient(Base):
    """Base record for each person entering the program."""
    __tablename__ = 'tbl_pt_demographics'
    __table_args__ = {'autoload':True}

    pt_id = al.Column("pt_id", al.String, primary_key=True)

    def __repr__(self):
        return "Patient %i: %s, %s"%(self.pt_id,self.anony_last_nm_txt,self.anony_first_nm_txt[0])

    def get_history(self):
        """Gives ordered list of procedures and exams."""
        d = np.array([e.exam_dt_datetime for e in self.exams]+[p.proc_dt_datetime for p in self.procedures])
        if len(d)>0:
            d[np.equal(d,None)] = datetime.date(3000,01,01)
            ind = d.argsort()
            res = []
            for i in ind:
                res.append((self.exams+self.procedures)[i])
            return res
        else: return []

class CAD(Base):
    """CAD Study record"""
    __tablename__ = "tbl_pt_mri_cad_record"
    __table_args__ = {'autoload':True}

    pt_mri_cad_record_id = al.Column("pt_mri_cad_record_id", al.String, primary_key=True)
    pt_id = al.Column("pt_id", al.String, al.ForeignKey("tbl_pt_demographics.pt_id"))

    patient = al.orm.relationship('Patient', backref=al.orm.backref('cad', uselist=False))

    def __repr__(self):
        return "CAD record %i: %s for patient %i"%(self.pt_mri_cad_record_id,self.latest_mutation_status_int,self.patient.pt_id)

class Exam(Base):
    """Record of imaging visit"""
    __tablename__ = 'tbl_pt_exam'
    __table_args__ = {'autoload':True}

    pt_exam_id = al.Column("pt_exam_id", al.String, primary_key=True)
    pt_id = al.Column("pt_id", al.String, al.ForeignKey("tbl_pt_demographics.pt_id"))
    exam_dt_datetime = al.Column("exam_dt_datetime", al.String)

    patient = al.orm.relationship('Patient', backref=al.orm.backref('exams', order_by=exam_dt_datetime))

    def __repr__(self):
        return "Exam %i: %s on %s; DICOM %s, CAD %s"%(self.pt_exam_id,self.exam_tp_int,self.exam_dt_datetime,self.exam_img_dicom_txt,
                                                      self.mri_cad_status_txt)

class Finding(Base):
    """Radiologist's evaluation of each suspicious feature in an imaging visit"""
    __tablename__ = 'tbl_pt_exam_finding'
    __table_args__ = {'autoload':True}

    pt_exam_finding_id = al.Column("pt_exam_finding_id", al.String, primary_key=True)
    pt_exam_id = al.Column("pt_exam_id", al.String, al.ForeignKey("tbl_pt_exam.pt_exam_id"))

    exam = al.orm.relationship('Exam', backref = al.orm.backref('findings'))

    birads_map = {None: 'Incomplete', 0: 'Incomplete', 1: 'Negative', 2: 'Benign',
                 3: 'Probably benign', 4: 'Suspicious abnormality', 5: 'Highly suggestive of malignancy',
                 6: 'Known biopsy/proven malignancy'}

    def __repr__(self):
        return "%i: %s finding (BIRADS %s)"%(self.pt_exam_finding_id,self.birads_map[self.all_birads_scr_int],self.all_birads_scr_int or "None")

class Procedure(Base):
    """An invasive action. May be radiological (e.g., fine needle)  or surgical (e.g.,
lumpectomy); may have associated imaging on-the-day; should have pathological
report(s)."""
    __tablename__ = 'tbl_pt_procedure'
    __table_args__ = {'autoload':True}

    pt_procedure_id = al.Column("pt_procedure_id", al.String, primary_key=True)
    pt_id = al.Column("pt_id", al.String, al.ForeignKey("tbl_pt_demographics.pt_id"))
    proc_dt_datetime = al.Column("proc_dt_datetime", al.String)

    patient = al.orm.relationship('Patient', backref=al.orm.backref('procedures', order_by=proc_dt_datetime))

    def __repr__(self):
        return "Procedure %i: %s on %s"%(self.pt_procedure_id,self.proc_source_int[:9],self.proc_dt_datetime)

class Pathology(Base):
    """A report on a single tissue sample. May be many per "procedure"."""
    __tablename__ = 'tbl_pt_pathology'
    __table_args__ = {'autoload':True}

    pt_path_id = al.Column("pt_path_id", al.String, primary_key=True)
    pt_procedure_id = al.Column("pt_procedure_id", al.String, al.ForeignKey("tbl_pt_procedure.pt_procedure_id"))

    procedure = al.orm.relationship('Procedure', backref=al.orm.backref('pathologies'))

    def get_diagnosis(self):
        if self.histop_core_biopsy_benign_yn:
            return "Benign"
        if self.histop_core_biopsy_high_risk_yn:
            return "High-risk"
        if self.histop_tp_isc_yn:
            return "In situ carcenoma"
        if self.histop_tp_ic_yn:
            return "Invasive carcenoma"
        return "Other malignancy"

    def __repr__(self):
        return "Pathology %i: cytology %s, histopath %s"%(self.pt_path_id,self.cytology_int,self.get_diagnosis())

class Series(Base):
    """
    Represents the tbl_pt_mri_series table. This is the one that is filled by
    the later code.
    """
    __tablename__='tbl_pt_mri_series'
    __table_args__ = {'autoload':True}
    pt_mri_series_id=sqlalchemy.Column(sqlalchemy.Integer,\
                            primary_key=True)

    a_no_txt = al.Column("a_no_txt",al.String, al.ForeignKey("tbl_pt_exam.a_number_txt"))
    exam = al.orm.relationship('Exam', backref=al.orm.backref('series'))

    def __repr__(self):
        return "Series %i: DICOM %s, accession %s"%(self.pt_mri_series_id,self.exam_img_dicom_txt,self.a_no_txt)

class Cancer(Base):
    """If pathology indicates cancer, this may be recorded here for selecting
    populations in the CAD study."""
    __tablename__ = 'tbl_pt_mri_cad_can_tp'
    __table_args__ = {'autoload':True}

    pt_mri_cad_can_tp_id = al.Column("pt_mri_cad_can_tp_id", al.String, primary_key=True)
    pt_path_id = al.Column("pt_path_id", al.String, al.ForeignKey("tbl_pt_pathology.pt_path_id"))

    pathology = al.orm.relationship('Pathology', backref=al.orm.backref('cancers'))

    def __repr__(self):
        return "Cancer %i: %s"%(self.pt_path_id,self.can_tp_int)

#Many-to-many relations
lesion_finding = al.Table('tbl_pt_exam_finding_lesion_link', Base.metadata,
     al.Column('pt_exam_finding_id', al.String, al.ForeignKey('tbl_pt_exam_finding.pt_exam_finding_id')),
     al.Column('pt_exam_lesion_id', al.String, al.ForeignKey('tbl_pt_exam_lesion.pt_exam_lesion_id')),
              autoload=True )

lesion_exam = al.Table('tbl_pt_exam_lesion_link', Base.metadata,
     al.Column('pt_exam_id', al.String, al.ForeignKey('tbl_pt_exam.pt_exam_id')),
     al.Column('pt_exam_lesion_id', al.String, al.ForeignKey('tbl_pt_exam_lesion.pt_exam_lesion_id')),
              autoload=True )

lesion_path = al.Table('tbl_pt_pathology_lesion_link', Base.metadata,
     al.Column('pt_path_id', al.String, al.ForeignKey('tbl_pt_pathology.pt_path_id')),
     al.Column('pt_exam_lesion_id', al.String, al.ForeignKey('tbl_pt_exam_lesion.pt_exam_lesion_id')),
              autoload=True )

class Lesion(Base):
    """Feedback of lesion analyses (pathology) to original detection (imaging), giving,
e.g., the location of the analysed lesion on the original image. Not yet linked up."""
    __tablename__ = 'tbl_pt_exam_lesion'
    __table_args__ = {'autoload':True}

    pt_exam_lesion_id = al.Column("pt_exam_lesion_id", al.String, primary_key=True)

    findings = al.orm.relationship('Finding', secondary=lesion_finding, backref=al.orm.backref("lesions"))
    exams = al.orm.relation('Exam', secondary=lesion_exam, backref=al.orm.backref('lesions'))
    pathologies = al.orm.relation('Pathology', secondary=lesion_path, backref=al.orm.backref('origins'))

    def __repr__(self):
        return "Lesion location %i "%(self.pt_exam_lesion_id)

def print_all(record):
    """Returns all the textual data for any given table entry"""
    out = ""
    for k in record.__table__.c.keys():
        out = out + str(k)+": "+str(record.__getattribute__(k)) + '\n'
    return out

def externals(entity):
    """Returns those attributes of an entity that point to one or more other
    entities (i.e., that represent relationships rather than data)."""
    allfields = [_ for _ in dir(entity) if (_[0]!='_') and not _ in entity.__table__.c.keys()]
    allfields.remove('metadata')
    if 'get_history' in allfields: allfields.remove('get_history')
    return allfields

## Utility funtcions below, for interactive use and special cases of pipeline input building
def get_path():
    q = s.query(Patient).options(al.orm.subqueryload_all('procedures.pathologies')).order_by(Patient.anony_last_nm_txt)
    all_patients = q.all()
    return [ (p,[(e,e.pathologies) for e in p.procedures]) for p in all_patients]

def all_histories():
    return [ (p,p.get_history()) for p in all_patients]

def bba():
    exams = s.query(Exam).filter(Exam.exam_tp_int == 'MRI').filter(Exam.sty_indicator_high_risk_yn==True).all()
    bba_list = [e for e in exams if e.mri_cad_status_txt=='Benign by assumption']
    return bba_list

def get_bbas(sat=''): #benign by assumption exams
    from numpy import array,loadtxt
    from matplotlib.mlab import find
    from prog.fetch_data.fetch import fetch
    import os
    os.chdir('/home/mdurant/data')
    try:
        plist = list(loadtxt('validation'+sat,usecols=[0]))
        fred = open('validation'+sat,'a')
    except IOError:
        plist = []
        fred = open('validation'+sat,'w')
    bba_list = bba()
    if sat: bba_list = bba_list[::-1]
    skip=0
    for anexam in bba_list:
        try:
            p = int(anexam.patient.cad.cad_pt_no_txt)
            if p in plist:
                print p,'already downloaded'
                skip=1
            if skip:
                skip=0
                continue
            elist = [ex for ex in anexam.patient.exams if (ex.sty_indicator_high_risk_yn==True)and(ex.exam_img_dicom_txt)][::-1]
            diffs = array([(anexam.exam_dt_datetime - e.exam_dt_datetime).days for e in elist if not(e.exam_img_dicom_txt is None)])
            if len(find(diffs>365))>0:
                ind = find(diffs>365)[0]
            else: continue
            otherexam = elist[ind]
            try:
                int1 = int(anexam.exam_img_dicom_txt)
                int2 = int(otherexam.exam_img_dicom_txt)
            except:
                continue
            fetch(p,int1)
            fetch(p,int2)
            fred.write("%i %i %i\n"%(p,int(anexam.exam_img_dicom_txt),int(otherexam.exam_img_dicom_txt)))
            fred.flush()
            plist.append(p)
        except KeyboardInterrupt:
            break
        except Exception, e:
            print(e)
            raise
    fred.close()


def useful():
    global all_screenings,all_patients,malignantfindings
    all_screenings = s.query(Patient,Exam).filter(Patient.pt_id==Exam.pt_id).filter(Exam.sty_indicator_high_risk_yn==True).filter(Exam.exam_tp_int == 'MRI')
    all_patients = q.all()
    malignantfindings = s.query(Exam).join(Finding).filter(Exam.exam_tp_int == 'MRI').filter( Finding.all_birads_scr_int.in_([4,5,6]) )

