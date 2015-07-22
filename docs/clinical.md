Clinical Data
================

Clinical data is meant for all kind of measurements not falling into other
categories. It can be data from questionnaires, physical body measurements or
socio-economic info about patient.


Parameters
------------
- `COLUMN_MAP_FILE` mandatory. Points to the column file. See below for format.
- `WORD_MAP_FILE` optional. Points to the file with dictionary to be used.
- `SECURITY_REQUIRED` optional. Y/N. Define study as private (Y) or Public (N).

COLUMN_MAP_FILE format
------------
Filename|Category Code|Column Number|Data Label|Data Label Source|Control Vocab
 Cd|Concept Type
Table, tab separated, txt file. It contains information about columns which are
to be uploaded into tranSMART.
- Filename  This column determines the file from where
column is located
- Category Code Path which contains the file
- Column Number Index of the column from the left beginning from 0
- Data Label  Label visible inside tranSMART after upload
- Data Label Source IGNORED skip if you don't need Concept Type Column
- Control Vocab cd  IGNORED skip if you don't need Concept Type Column
- Concept Type  Use this concept type instead of inferring it from the first row
