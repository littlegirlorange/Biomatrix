"""
Pulls exam reports off BioMatrix.
SQL commands and BioMatrix table mapping taken from Martin's BREAST program.
Modified for use outside of BREAST by Maggie Kusano, February 3, 2016.
"""

import os
from loadsavepars import pars
import biomat_syntax as bm
from fetch_data.sql_run import Session


def build_tasklist(task_file):
    '''
    Builds a list of CAD patient IDs and associated accession numbers to process from a text file
    (input to PACSPull and pipeline)
    :param task_file:
        Full path to text file listing studies to pull from.  The text file is in the same format as the text
        files used for PACSPull and the BreastCAD pipeline (a tab-separated list of studies):
        CADPatID    StudyDate   AccessionNumber
    :return:
        List of studies to pull. Each item is in the form:
        [CADPatID, FixedAccessionNumber, MovingAccessionNumber]
    '''
    fileobj = open(task_file, "r")
    list = []
    try:
        for line in fileobj:
            # Get the study and accession number.
            lineparts = line.split()
            list.append([lineparts[0], lineparts[2]])
    finally:
        fileobj.close()

    list = sorted(list, key=lambda x: (x[0], x[1]), reverse=True)

    tasklist = []
    task = []
    seen = set()
    fixed_accession = ""
    for line in list:
        study = line[0]
        accession = line[1]
        if study not in seen:
            seen.add(study)
            task.append(study)
            task.append(accession)
            fixed_accession = accession
        elif len(task) == 2:
            task.append(accession)
            tasklist.append(task)
            task = []
        else:
            task.append(study)
            task.append(fixed_accession)
            task.append(accession)
            tasklist.append(task)
            task = []
    return tasklist

tasklist = build_tasklist(pars['task_file'])
session = Session()

for iItem, item in enumerate(tasklist):

    cad_pat_no = item[0]
    accession_no_fixed = item[1]
    accession_no_moving = item[2]
    cad_pat_no_str = str(cad_pat_no).zfill(4)

    print "=========================================================================="
    print "CADPat no.: " + cad_pat_no_str + ", Fixed Accession no.: " + accession_no_fixed + ", Moving Accession no.: " + accession_no_moving
    output_dir = pars['output_dir']# + os.sep + cad_pat_no + "_" + accession_no_fixed + "_" + accession_no_moving
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    query_filter = ("CAD.cad_pt_no_txt=='{0}'").format(cad_pat_no_str)
    cad_entries = bm.process_query(query_filter, session)

    if len(cad_entries) > 1:
        print "Multiple CAD entries found. Returning."
        exit

    exams = cad_entries[0].patient.exams

    for exam in exams:
        if exam.a_number_txt in [accession_no_fixed, accession_no_moving]:
            exam_date = str(exam.exam_dt_datetime.year) + \
                        str(exam.exam_dt_datetime.month).zfill(2) + \
                        str(exam.exam_dt_datetime.day).zfill(2)
            out_file = output_dir + os.sep + cad_pat_no + "CADPat_" + \
                       exam.exam_img_dicom_txt + "_" + \
                       exam_date + "_" + \
                       exam.a_number_txt + "_report.txt"
            f = open(out_file, "w")
            f.write("CADPat     = " + cad_pat_no_str + "\n")
            f.write("Accession  = " + exam.a_number_txt + "\n")
            f.write("Study date = " + str(exam.exam_dt_datetime) + "\n")
            f.write(exam.original_report_txt)
            f.close()
            print "CADPat     = " + cad_pat_no_str
            print "Accession  = " + exam.a_number_txt
            print "Study date = " + str(exam.exam_dt_datetime)
            print exam.original_report_txt
            print ""



