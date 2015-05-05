package org.transmartproject.batch.i2b2.dimensions

import org.springframework.batch.item.ItemWriter
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.jdbc.core.simple.SimpleJdbcInsert
import org.springframework.stereotype.Component
import org.transmartproject.batch.beans.JobScopeInterfaced
import org.transmartproject.batch.clinical.db.objects.Tables
import org.transmartproject.batch.db.DatabaseUtil
import org.transmartproject.batch.i2b2.misc.I2b2ControlColumnsHelper

/**
 * Writes patients into patient_dimension and patient_mapping.
 */
@Component
@JobScopeInterfaced
class InsertPatientsWriter implements ItemWriter<DimensionsStoreEntry> {

    @Value(Tables.PATIENT_DIMENSION)
    private SimpleJdbcInsert patientDimensionInsert

    @Value(Tables.PATIENT_MAPPING)
    private SimpleJdbcInsert patientMappingInsert

    @Value("#{jobParameters['PATIENT_IDE_SOURCE']}")
    private String patientIdeSource

    @Autowired
    private I2b2ControlColumnsHelper i2b2ControlColumnsHelper

    @Override
    void write(List<? extends DimensionsStoreEntry> items) throws Exception {
        int[] res
        res = patientDimensionInsert.executeBatch(
                items.collect {
                    [
                            patient_num: it.internalId as Long,
                            *: i2b2ControlColumnsHelper.controlValues,
                    ]
                } as Map[])
        DatabaseUtil.checkUpdateCounts(res,
                "inserting into $Tables.PATIENT_DIMENSION")

        res = patientMappingInsert.executeBatch(
                items.collect { [
                        patient_ide: it.externalId,
                        patient_ide_source: patientIdeSource,
                        patient_num: it.internalId as Long,
                        *: i2b2ControlColumnsHelper.controlValues,
                    ]
                } as Map[])
        DatabaseUtil.checkUpdateCounts(res,
                "inserting into $Tables.PATIENT_MAPPING")
    }
}
