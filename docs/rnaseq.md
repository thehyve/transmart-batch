# RNASeq

## Platform Upload

Your platform configuration file has to have the `rnaseq_annotation.params` name.
The content of the params file is the same as for other HD data types.

For format of the platform data file see [chromosomal_region.md](chromosomal_region.md)

## RNASeq Data Upload

Your must have `rnaseq.params` file. For possible parameter see [hd-params.md](hd-params.md).

Below is the expected file format for RNASeq data input files.

| Column Name | Description |
--------------|--------------
| GENE_SYMBOL | **Mandatory** The name of this gene. e.g. `WASH7P` |
| SAMPLE | **Mandatory** Sample id. e.g. `CACO2` |
| READCOUNT | **Mandatory** Actual measurement. |
| NORM_READCOUNT | *Optional* Normalized readcount. (e.g. RPKM) |


