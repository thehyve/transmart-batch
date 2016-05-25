package org.transmartproject.batch.batchartifacts

import org.junit.Test
import org.springframework.batch.item.ExecutionContext
import org.springframework.batch.item.ItemStreamReader
import org.springframework.batch.item.file.FlatFileItemReader
import org.springframework.batch.item.file.mapping.DefaultLineMapper
import org.springframework.batch.item.file.mapping.FieldSetMapper
import org.springframework.batch.item.file.separator.DefaultRecordSeparatorPolicy
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer
import org.springframework.batch.item.file.transform.FieldSet
import org.springframework.batch.item.file.transform.LineAggregator
import org.springframework.batch.item.file.transform.PassThroughFieldExtractor
import org.springframework.core.io.ByteArrayResource
import org.springframework.core.io.Resource

import static org.hamcrest.MatcherAssert.assertThat
import static org.hamcrest.Matchers.*

/**
 * Tests the {@link CsvLineAggregator}, to see if it writes lines such that
 * they can be read correctly by the {@link DelimitedLineTokenizer}.
 */
class CsvLineAggregatorTests {

    static <T> void write(List<? extends T> items, LineAggregator<T> lineAggregator, Writer writer) {
        StringBuilder lines = new StringBuilder()
        for (T item : items) {
            lines.append(lineAggregator.aggregate(item))
        }
        writer.write(lines.toString())
    }

    static <T> List<T> read(ItemStreamReader<T> reader) {
        List<T> result = []
        reader.open(new ExecutionContext())
        List<String> item
        while ((item = reader.read()) != null) {
            result << item
        }
        reader.close()
        result
    }

    private static final List<List<String>> TEST_DATA = [
            [ 'Column 1', 'Column 2', 'Column 3', 'Column 4' ],
            [ 'A', 'Test', 'Test with tab (\t).', 'Test should pass.' ],
            [ 'B', 'Test', 'Test with comma (,) in the text.', 'Test should pass.' ],
            [ 'C', 'Test', 'Test with newline (\n) in it.', 'Test should pass.' ],
    ]

    @Test
    void testWriteCsvData() {
        // Writing test data to byte array
        ByteArrayOutputStream out = new ByteArrayOutputStream()
        out.withWriter { writer ->
            def aggregator = new CsvLineAggregator<List<String>>(
                    separator: '\t',
                    fieldExtractor: new PassThroughFieldExtractor<List<String>>(),
            )
            write(TEST_DATA, aggregator, writer)
            writer.flush()
        }

        // Reading data from byte array
        def tokenizer = new DelimitedLineTokenizer(
                delimiter: '\t',
        )
        def lineMapper = new DefaultLineMapper(
                lineTokenizer: tokenizer,
                fieldSetMapper: { FieldSet fs ->
                    fs.values as List<String>
                } as FieldSetMapper<List<String>>,
        )

        Resource resource = new ByteArrayResource(out.toByteArray())
        def reader = new FlatFileItemReader<List<String>>(
                lineMapper: lineMapper,
                recordSeparatorPolicy: new DefaultRecordSeparatorPolicy(),
                resource: resource,
        )
        reader.afterPropertiesSet()
        List<List<String>> result = read(reader)

        assertThat result, equalTo(TEST_DATA)
    }

}
