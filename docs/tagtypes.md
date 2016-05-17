Upload tag types
-----------------------------

Tag types can be loaded that are used during [loading of metadata tags](tags.md).
When tag types have been loaded and a tag is loaded with the same title as an existing tag type,
then the value of the tag is validated to belong to the loaded set of valid values.

Parameters
------------
The parameters file should be named `tagtypes.params` and contains:
- `TAG_TYPES_FILE` **Mandatory**. Points to the tag types file. See below for format.

#####Tag types tsv input file format

|`node_type`|`title`    |`solr_field_name`|`value_type`         |`shown_if_empty`|`values`                 |`index`|
|-----------|-----------|-----------------|---------------------|----------------|-------------------------|-------|
|`STUDY`    |Organism   |`organism`       |`NON_ANALYZED_STRING`|`Y`             |Homo sapiens             |1      |
|`STUDY`    |Study phase|`study_phase`    |`NON_ANALYZED_STRING`|`N`             |Phase 0,Phase I          |2      |

The `title` column maps onto the `tag_type` column of the `i2b2metadata.i2b2_tag_types` table,
values are stored in `i2b2metadata.i2b2_tag_options`.

Header names are not strict, but header has to be present because first line is always skipped.
The order of the columns is important.

- `node_type` &ndash; the type of node (`ALL`, `STUDY`, `FOLDER`, `CATEGORICAL`, `NUMERICAL`, `HIGHDIM`, _any high dim data type_)
- `title` &ndash; title of the tag. e.g. &lsquo;Organism&rsquo;.
- `solr_field_name` &ndash; lowercase and whitespace free variant of the title for use in Solr.
- `value_type` &ndash; the type of the values (`DATE`, `NON_ANALYZED_STRING`, `ANALYZED_STRING`, `INTEGER`, `FLOAT`). Currently
only `NON_ANALYZED_STRING` is supported.
- `shown_if_empty` &ndash; determines if the tag type will show up in the metadata popup if no tag for the concept is loaded (`Y`, `N`).
- `values` &ndash; comma-separated list of allowed values of tags of the type.
- `index` &ndash; detects position of tags on popup relatively to others. A higher position in tags with lower number.

#####Tag types upload

* Place the tag types file into `global/tagtypes` folder.
* You must specify the tag types file using the `TAGS_FILE` variable inside the `global/tagtypes.params` file.
* Run

    `./transmart-batch-capsule.jar -p global/tagtypes.params`

#####Browse tags export

Existing browse tags and associated concepts can be exported using the command:

    ./transmart-batch-capsule.jar -p /path/to/STUDY_NAME/browsetagsexport.params

Put this in the file `/path/to/STUDY_NAME/browsetagsexport.params`:
```
EXPORT_BROWSE_TAGS_FILE=browsetags.export.txt
EXPORT_BROWSE_TAG_TYPES_FILE=browsetagtypes.export.txt
```
This will produce these two files in `/path/to/STUDY_NAME/browsetagsexport`, containing data that can be imported
by a tag types loading job.
The `EXPORT_BROWSE_TAG_TYPES_FILE` variable is optional.

#####Tag types deletion

When loading a tag types file when already tag types have been loaded, tag types that do not appear in the new file
will be deleted. If there are still references to a tag types that is to be deleted, an exception is thrown.
