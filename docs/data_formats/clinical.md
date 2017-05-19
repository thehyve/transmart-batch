Clinical Data
================

Clinical data is meant for all kind of measurements not falling into other
categories. It can be data from questionnaires, physical body measurements or
socio-economic info about patient.


Parameters
------------
- `COLUMN_MAP_FILE` **Mandatory**. Points to the column file. See below for format.
- `WORD_MAP_FILE` Points to the file with dictionary to be used.
- `XTRIAL_FILE` Points to [the cross study concepts file](xtrial.md).
- `TAGS_FILE` Points to [the concepts tags file](tags.md).

You could also use [the study-specific parameters](study-params.md).

COLUMN_MAP_FILE format
------------

|Filename|Category Code|Column Number|Data Label|Data Label Source|Control Vocab Cd|Concept Type |
|--------|-------------|-------------|----------|-----------------|----------------|-------------|
|data.txt|path\to\label|1            |Age       |                 |                |NUMERICAL    |
Table, tab separated, txt file. It contains information about columns which are
to be uploaded into tranSMART.
- `Filename`  This column determines the file where column is located.
- `Category Code` Path in concept tree between the study node and the data label, 
subdirectories can be represented by joining names with a divider sign (plus `+` or back slash `\`).
To place a concept directly within the study directory, enter only a divider sign.
- `Column Number` Index of the column from the left beginning from 1.
- `Data Label`  Label visible inside tranSMART after upload.
- `Data Label Source` works for template column only (has `\` in data label column). 
Refers to the data label column where to get data for dynamic concept path generation. 
See [templates](templates.md) documentation for more details.
- `Control Vocab cd`  IGNORED skip if you don't need Concept Type Column.
- `Concept Type`  Use this concept type instead of inferring it from the first row.

Allowed values for Concept type:
- `NUMERICAL` for numerical
- `CATEGORICAL` for text
