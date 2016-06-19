package org.transmartproject.batch.highdim.vcf

import htsjdk.samtools.util.CloseableIterator
import htsjdk.variant.variantcontext.VariantContext
import htsjdk.variant.vcf.VCFFileReader
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader
import org.springframework.core.io.Resource
import org.springframework.util.ClassUtils

/**
 * Reads vcf file
 */
class VariantItemReader extends AbstractItemCountingItemStreamItemReader<VariantContext> {

    Resource vcfResource

    private CloseableIterator<VariantContext> iterator

    VariantItemReader() {
        setName(ClassUtils.getShortName(VariantItemReader))
    }

    @Override
    protected VariantContext doRead() throws Exception {
        if (iterator.hasNext()) {
            iterator.next()
        } else {
            null
        }
    }

    @Override
    protected void doOpen() throws Exception {
        assert vcfResource
        iterator = new VCFFileReader(vcfResource.getFile(), false).iterator()
    }

    @Override
    protected void doClose() throws Exception {
        if (iterator) {
            iterator.close()
        }
    }
}
