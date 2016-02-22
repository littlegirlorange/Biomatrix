
"""This package provides interaction with the SQL databade in
order to generate suitable MRI exam lists to feeding through the 
registration/analysis pipeline. These exam lists (studyID/examID)
will then be fetchable from the PACS in DICOM format.
Note that SQL and PACS credentials are required."""

#TODO: also need a module to set up credentials for new installations.