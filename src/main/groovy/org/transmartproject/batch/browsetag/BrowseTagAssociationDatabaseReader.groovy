package org.transmartproject.batch.browsetag

import groovy.util.logging.Slf4j
import org.springframework.batch.item.ItemStreamReader
import org.springframework.batch.item.database.JdbcCursorItemReader
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.jdbc.core.PreparedStatementSetter
import org.springframework.jdbc.core.RowMapper
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.transmartproject.batch.clinical.db.objects.Tables

import javax.annotation.PostConstruct
import javax.sql.DataSource
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException

/**
 * Gets the browse tags, associated with folders, from database.
 */
@Slf4j
class BrowseTagAssociationDatabaseReader implements ItemStreamReader<BrowseTagAssociation> {

    @Value("#{jobParameters['STUDY_ID']}")
    String studyId

    @Autowired
    NamedParameterJdbcTemplate jdbcTemplate

    @Delegate
    JdbcCursorItemReader<BrowseTagAssociation> delegate

    @Autowired
    DataSource dataSource

    @PostConstruct
    void init() {
        delegate = new JdbcCursorItemReader<>(
                driverSupportsAbsolute: true,
                dataSource: dataSource,
                sql: sql,
                preparedStatementSetter: this.&setStudyId as PreparedStatementSetter,
                rowMapper: this.&mapRow as RowMapper<BrowseTagAssociation>)

        delegate.afterPropertiesSet()
    }

    void setStudyId(PreparedStatement ps) throws SQLException {
        log.info "Study ID: ${studyId}"
        ps.setString(1, studyId)
        ps.setString(2, studyId)
        ps.setString(3, studyId)
        ps.setString(4, studyId)
    }

    static final String TAG_ITEMS_QUERY =
            """
            (SELECT
                f.folder_id,
                f.folder_name,
                f.folder_full_name,
                f.folder_type,
                f.description as folder_description,
                ati.tag_item_id,
                cast (ati.code_type_name as varchar(200)) as code,
                cast (ati.display_name as varchar(200)) as display_name,
                ati.display_order as display_order,
                cast (tv.value as varchar(4000)) as value,
                cast (tv.value as varchar(4000)) as description
            FROM $Tables.FM_FOLDER f
            INNER JOIN $Tables.FM_FOLDER_ASSOCIATION fa
            ON f.folder_id = fa.folder_id
            INNER JOIN $Tables.FM_DATA_UID fuid
            ON f.folder_id = fuid.fm_data_id
            INNER JOIN $Tables.AM_TAG_ASSOCIATION ata
            ON fuid.unique_id = ata.subject_uid
            INNER JOIN $Tables.AM_TAG_ITEM ati
            ON ata.tag_item_id = ati.tag_item_id
            INNER JOIN $Tables.AM_DATA_UID tuid
            ON ata.object_uid = tuid.unique_id
            INNER JOIN $Tables.AM_TAG_VALUE tv
            ON tuid.am_data_id = tv.tag_value_id
            WHERE ata.object_type = 'AM_TAG_VALUE'
            AND f.folder_type = 'STUDY'
            AND UPPER(fa.object_uid) = concat('EXP:', ?)
            )
            """
    static final String CONCEPT_CODES_QUERY =
            """
            (SELECT
                f.folder_id,
                f.folder_name,
                f.folder_full_name,
                f.folder_type,
                f.description as folder_description,
                ati.tag_item_id,
                cast (ati.code_type_name as varchar(200)) as code,
                cast (ati.display_name as varchar(200)) as display_name,
                -1 as display_order,
                cast (bcc.bio_concept_code as varchar(4000)) as value,
                cast (bcc.code_description as varchar(4000)) as description
            FROM $Tables.FM_FOLDER f
            INNER JOIN $Tables.FM_FOLDER_ASSOCIATION fa
            ON f.folder_id = fa.folder_id
            INNER JOIN $Tables.FM_DATA_UID fuid
            ON f.folder_id = fuid.fm_data_id
            INNER JOIN $Tables.AM_TAG_ASSOCIATION ata
            ON fuid.unique_id = ata.subject_uid
            INNER JOIN $Tables.AM_TAG_ITEM ati
            ON ata.tag_item_id = ati.tag_item_id
            INNER JOIN $Tables.BIO_CONCEPT_CODE bcc
            ON ata.object_uid = concat(bcc.code_type_name, concat(':', bcc.bio_concept_code))
            WHERE ata.object_type = 'BIO_CONCEPT_CODE'
            AND f.folder_type = 'STUDY'
            AND UPPER(fa.object_uid) = concat('EXP:', ?)
            )
            """
    static final String DISEASES_QUERY =
            """
            (SELECT
                f.folder_id,
                f.folder_name,
                f.folder_full_name,
                f.folder_type,
                f.description as folder_description,
                ati.tag_item_id,
                cast (ati.code_type_name as varchar(200)) as code,
                cast (ati.display_name as varchar(200)) as display_name,
                -1 as display_order,
                cast (bd.disease as varchar(4000)) as value,
                cast (bd.prefered_name as varchar(4000)) as description
            FROM $Tables.FM_FOLDER f
            INNER JOIN $Tables.FM_FOLDER_ASSOCIATION fa
            ON f.folder_id = fa.folder_id
            INNER JOIN $Tables.FM_DATA_UID fuid
            ON f.folder_id = fuid.fm_data_id
            INNER JOIN $Tables.AM_TAG_ASSOCIATION ata
            ON fuid.unique_id = ata.subject_uid
            INNER JOIN $Tables.AM_TAG_ITEM ati
            ON ata.tag_item_id = ati.tag_item_id
            INNER JOIN $Tables.BIO_DISEASE bd
            ON ata.object_uid = concat('DIS:', bd.mesh_code)
            WHERE ata.object_type = 'BIO_DISEASE'
            AND f.folder_type = 'STUDY'
            AND UPPER(fa.object_uid) = concat('EXP:', ?)
            )
            """
    static final String EXPERIMENTS_QUERY =
            """
            (SELECT
                f.folder_id,
                f.folder_name,
                f.folder_full_name,
                f.folder_type,
                f.description as folder_description,
                -1 as tag_item_id,
                'study_description' as code,
                'Study description' as display_name,
                1 as display_order,
                cast (exp.description as varchar(4000)) as value,
                cast (exp.description as varchar(4000)) as description
            FROM $Tables.FM_FOLDER f
            INNER JOIN $Tables.FM_FOLDER_ASSOCIATION fa
            ON f.folder_id = fa.folder_id
            INNER JOIN $Tables.BIO_EXPERIMENT exp
            ON fa.object_uid = concat('EXP:', exp.accession)
            WHERE f.folder_type = 'STUDY'
            AND UPPER(fa.object_uid) = concat('EXP:', ?)
            )
            """

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
        $TAG_ITEMS_QUERY
        UNION
        $CONCEPT_CODES_QUERY
        UNION
        $DISEASES_QUERY
        UNION
        $EXPERIMENTS_QUERY
        """
    }

    private final Map<String, BrowseFolderType> folderTypes = [:]

    private BrowseFolderType getFolderType(ResultSet rs) {
        String folderTypeName = rs.getString('folder_type')
        BrowseFolderType folderType = folderTypes[folderTypeName]
        if (folderType == null) {
            folderType = new BrowseFolderType(
                    type: folderTypeName,
                    displayName: rs.getString('folder_description')
            )
            folderTypes[folderTypeName] = folderType
        }
        folderType
    }

    private final Map<Long, BrowseTagType> tagTypes = [:]

    private BrowseTagType getTagType(ResultSet rs) {
        Long tagItemId = rs.getLong('tag_item_id')
        BrowseTagType tagType = tagTypes[tagItemId]
        if (tagType == null) {
            tagType = new BrowseTagType(
                    folderType: getFolderType(rs),
                    code: rs.getString('code'),
                    displayName: rs.getString('display_name'),
            )
            tagTypes[tagItemId] = tagType
        }
        tagType
    }

    private final Map<String, BrowseFolder> folders = [:]

    private BrowseFolder getFolder(ResultSet rs) {
        String fullName = rs.getString('folder_full_name')
        BrowseFolder folder = folders[fullName]
        if (folder == null) {
            folder = new BrowseFolder(
                    fullName: fullName,
                    id: rs.getLong('folder_id'),
                    name: rs.getString('folder_name'),
                    type: getFolderType(rs),
            )
            folders[fullName] = folder
        }
        folder
    }

    private BrowseTagValue getValue(ResultSet rs) {
        new BrowseTagValue(
                type: getTagType(rs),
                value: rs.getString('value'),
                description: rs.getString('description')
        )
    }

    @SuppressWarnings('UnusedPrivateMethodParameter')
    private BrowseTagAssociation mapRow(ResultSet rs, int rowNum) throws SQLException {
        new BrowseTagAssociation(
                folder: getFolder(rs),
                value: getValue(rs),
                index: rs.getInt('display_order') < 0 ? null : rs.getInt('display_order')
        )
    }

}

