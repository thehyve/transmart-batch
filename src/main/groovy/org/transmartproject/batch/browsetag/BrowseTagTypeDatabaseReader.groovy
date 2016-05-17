package org.transmartproject.batch.browsetag

import groovy.util.logging.Slf4j
import org.springframework.batch.item.ItemStreamReader
import org.springframework.batch.item.database.JdbcCursorItemReader
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.RowMapper
import org.transmartproject.batch.clinical.db.objects.Tables

import javax.annotation.PostConstruct
import javax.sql.DataSource
import java.sql.ResultSet
import java.sql.SQLException

/**
 * Gets the browse tag types from database.
 */
@Slf4j
class BrowseTagTypeDatabaseReader implements ItemStreamReader<BrowseTagType> {

    @Delegate
    JdbcCursorItemReader<BrowseTagType> delegate

    @Autowired
    DataSource dataSource

    @Autowired
    JdbcTemplate jdbcTemplate

    private final Map<String, BrowseFolderType> folderTypesCache = [:]

    private final Map<Long, BrowseTagType> tagTypesCache = [:]

    @PostConstruct
    void init() {
        delegate = new JdbcCursorItemReader<>(
                driverSupportsAbsolute: true,
                dataSource: dataSource,
                sql: sql,
                rowMapper: this.&mapRow as RowMapper<BrowseTagType>)

        delegate.afterPropertiesSet()
    }

    /**
     * Query for fetching all tag types with concept codes attached
     * to them.
     */
    private String getSql() {
        /*
            Table {@link $Tables.BIO_CONCEPT_CODE}:
            primary key: bio_concept_code_id
            unique: (code_type_name, bio_concept_code)
            index: code_type_name

            Table {@link $Tables.AM_TAG_ITEM}:
            primary key: (tag_template_id, tag_item_id)
         */

        """
                SELECT DISTINCT
                    bcc.code_type_name,
                    ati.tag_template_id,
                    ati.tag_item_id,
                    ati.tag_item_uid,
                    ati.display_name,
                    ati.tag_item_type,
                    ati.tag_item_subtype,
                    ati.code_type_name,
                    ati.required,
                    att.tag_template_name,
                    att.tag_template_type,
                    att.tag_template_subtype
                FROM $Tables.BIO_CONCEPT_CODE bcc
                INNER JOIN $Tables.AM_TAG_ITEM ati
                ON bcc.code_type_name = ati.code_type_name
                INNER JOIN $Tables.AM_TAG_TEMPLATE att
                ON ati.tag_template_id = att.tag_template_id
                ORDER BY display_name ASC
        """
    }

    /**
     * Query for fetching all concept codes for a certain tag type.
     * @param tagType the tag type.
     */
    private String getValuesSql(BrowseTagType tagType) {
        /*
            Table {@link $Tables.BIO_CONCEPT_CODE}:
            primary key: bio_concept_code_id
            unique: (code_type_name, bio_concept_code)
            index: code_type_name

            Table {@link $Tables.AM_TAG_ITEM}:
            primary key: (tag_template_id, tag_item_id)
         */
        """
                SELECT
                    bcc.bio_concept_code_id,
                    bcc.bio_concept_code,
                    bcc.code_name,
                    bcc.code_description,
                    bcc.code_type_name,
                    bcc.filter_flag,
                    ati.tag_template_id,
                    ati.tag_item_id,
                    ati.tag_item_uid,
                    ati.display_name,
                    ati.tag_item_type,
                    ati.tag_item_subtype,
                    ati.code_type_name,
                    ati.required,
                    att.tag_template_name,
                    att.tag_template_type,
                    att.tag_template_subtype
                FROM $Tables.BIO_CONCEPT_CODE bcc
                INNER JOIN $Tables.AM_TAG_ITEM ati
                ON bcc.code_type_name = ati.code_type_name
                INNER JOIN $Tables.AM_TAG_TEMPLATE att
                ON ati.tag_template_id = att.tag_template_id
                WHERE ati.tag_item_id = $tagType.id
                ORDER BY bcc.code_description ASC
        """
    }

    private BrowseFolderType getFolderType(ResultSet rs) {
        String tagTemplateType = rs.getString('tag_template_type')
        BrowseFolderType folderType = folderTypesCache[tagTemplateType]
        if (folderType == null) {
            folderType = new BrowseFolderType(
                    type: tagTemplateType,
                    displayName: rs.getString('tag_template_name')
            )
            folderTypesCache[tagTemplateType] = folderType
        }
        folderType
    }

    private BrowseTagType getTagType(ResultSet rs) {
        Long tagItemId = rs.getLong('tag_item_id')
        BrowseTagType tagType = tagTypesCache[tagItemId]
        if (tagType == null) {
            tagType = new BrowseTagType(
                    id: tagItemId,
                    code: rs.getString('code_type_name'),
                    folderType: getFolderType(rs),
                    type: rs.getString('tag_item_type'),
                    subType: rs.getString('tag_item_subtype'),
                    displayName: rs.getString('display_name'),
                    required: rs.getBoolean('required')
            )
            tagTypesCache[tagItemId] = tagType
        }
        tagType
    }

    @SuppressWarnings('UnusedPrivateMethodParameter')
    private BrowseTagType mapRow(ResultSet rs, int rowNum) throws SQLException {
        def tagType = getTagType(rs)
        Collection<String> values = jdbcTemplate.query(getValuesSql(tagType),
                { ResultSet valuesRs, int valuesRowNum ->
                    valuesRs.getString('code_description')
                } as RowMapper<String>
        )
        tagType.values = values
        tagType.index = rowNum
        tagType
    }

}

