package org.transmartproject.batch.highdim.maf

import groovy.util.logging.Slf4j
import org.springframework.batch.item.ItemStreamReader
import org.springframework.batch.item.database.JdbcCursorItemReader
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.PreparedStatementSetter
import org.springframework.jdbc.core.RowMapper
import org.transmartproject.batch.beans.JobScopeInterfaced
import org.transmartproject.batch.clinical.db.objects.Tables
import org.transmartproject.batch.highdim.platform.Platform

import javax.annotation.PostConstruct
import javax.sql.DataSource
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException

/**
 * Reads a MAF mutation events from the database
 */
@JobScopeInterfaced
@Slf4j
class MafMutationEventsReader implements ItemStreamReader<MutationEvent> {

    @Autowired
    Platform platform

    @Delegate
    JdbcCursorItemReader<MutationEvent> delegate

    @Autowired
    DataSource dataSource

    @PostConstruct
    void init() {
        delegate = new JdbcCursorItemReader<>(
                sql: sql,
                dataSource: dataSource,
                preparedStatementSetter: { PreparedStatement ps ->
                    ps.setString(1, platform.id)

                } as PreparedStatementSetter,
                rowMapper: new MutationEventRowMapper(),
                saveState: false,
        )
    }

    private String getSql() {
        "SELECT * FROM ${Tables.MAF_MUTATION_EVENT} WHERE gpl_id = ?"
    }

    static class MutationEventRowMapper implements RowMapper<MutationEvent> {
        @Override
        MutationEvent mapRow(ResultSet rs, int rowNum) throws SQLException {
            new MutationEvent(
                    mutationEventId: rs.getLong('mutation_event_id'),
                    gplId: rs.getString('gpl_id'),
                    entrezGeneId: rs.getInt('entrez_gene_id'),
                    chr: rs.getString('chr'),
                    startPosition: rs.getLong('start_position'),
                    endPosition: rs.getLong('end_position'),
                    referenceAllele: rs.getString('reference_allele'),
                    tumorSeqAllele: rs.getString('tumor_seq_allele'),
                    proteinChange: rs.getString('protein_change'),
                    mutationType: rs.getString('mutation_type'),
                    functionalImpactScore: rs.getString('functional_impact_score'),
                    fisValue: rs.getLong('fis_value'),
                    linkXvar: rs.getString('link_xvar'),
                    linkPdb: rs.getString('link_pdb'),
                    linkMsa: rs.getString('link_msa'),
                    ncbiBuild: rs.getString('ncbi_build'),
                    strand: rs.getString('strand'),
                    variantType: rs.getString('variant_type'),
                    dbSnpRs: rs.getString('db_snp_rs'),
                    dbSnpValStatus: rs.getString('db_snp_val_status'),
                    oncotatorDbsnpRs: rs.getString('oncotator_dbsnp_rs'),
                    oncotatorRefseqMrnaId: rs.getString('oncotator_refseq_mrna_id'),
                    oncotatorCodonChange: rs.getString('oncotator_codon_change'),
                    oncotatorUniprotEntryName: rs.getString('oncotator_uniprot_entry_name'),
                    oncotatorUniprotAccession: rs.getString('oncotator_uniprot_accession'),
                    oncotatorProteinPosStart: rs.getLong('oncotator_protein_pos_start'),
                    oncotatorProteinPosEnd: rs.getLong('oncotator_protein_pos_end'),
            )
        }
    }
}
