package org.transmartproject.batch.highdim.vcf

import htsjdk.variant.variantcontext.Genotype
import htsjdk.variant.variantcontext.VariantContext
import org.springframework.batch.core.configuration.annotation.JobScope
import org.springframework.batch.item.ItemWriter
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.jdbc.core.simple.SimpleJdbcInsert
import org.springframework.stereotype.Component
import org.transmartproject.batch.clinical.db.objects.Sequences
import org.transmartproject.batch.clinical.db.objects.Tables
import org.transmartproject.batch.db.SequenceReserver

/**
 * Writes variants into vcf data table.
 */
@Component
@JobScope
class InsertVariantWriter implements ItemWriter<VariantContext> {

    @Value(Tables.VCF_DATA)
    private SimpleJdbcInsert simpleJdbcInsert

    @Autowired
    SequenceReserver sequenceReserver

    @Value("#{vcfContextItems.sampleCodeAssayIdMap}")
    Map<String, Long> sampleCodeAssayIdMap

    @Value("#{jobExecution.executionContext.get('datasetId')}")
    String datasetId

    @Override
    void write(List<? extends VariantContext> items) throws Exception {
        simpleJdbcInsert.executeBatch(
            items.collectMany { VariantContext context ->
                def commonPart = [
                        chr         : context.contig,
                        pos         : context.start,
                        dataset_id  : datasetId,
                        rs_id       : context.ID,
                        variant_type: context.type.name(),
                        ref         : context.reference.displayString,
                        alt         : (context.alternateAlleles*.displayString).join(', '),
                        //qual: qual,
                        filter      : context.filters.join(', '),
                        info        : context.attributes.collect { k, v -> "$k=$v" }.join(';'),
                        //format
                        //variant_value: context
                        //gene_name: context,
                        //gene_id: context,
                ]
                context.genotypes.collect { Genotype genotype ->
                    def genotypeMap = commonPart.clone()
                    genotypeMap['variant_subject_summary_id'] = sequenceReserver.getNext(Sequences.VCF_ID)
                    //variant: 'A/G',
                    //variant_format: 'R/V',
                    //reference
                    if (genotype.alleles) {
                        genotypeMap['allele1'] = context.getAlleleIndex(genotype.alleles[0])
                        if (genotype.alleles.size() > 1) {
                            genotypeMap['allele2'] = context.getAlleleIndex(genotype.alleles[1])
                        }
                    }
                    genotypeMap['assay_id'] = sampleCodeAssayIdMap[genotype.sampleName]
                    genotypeMap
                }
            } as Map[])
    }
}
