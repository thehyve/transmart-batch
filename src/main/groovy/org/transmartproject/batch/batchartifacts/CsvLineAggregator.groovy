package org.transmartproject.batch.batchartifacts

import com.opencsv.CSVWriter
import groovy.transform.CompileStatic
import org.springframework.batch.item.file.transform.ExtractorLineAggregator

/**
 * Writes lines in CSV format using the opencsv library.
 */
@CompileStatic
class CsvLineAggregator<T> extends ExtractorLineAggregator<T> {

    char separator = CSVWriter.DEFAULT_SEPARATOR
    char quotechar = CSVWriter.DEFAULT_QUOTE_CHARACTER
    char escapechar = CSVWriter.DEFAULT_ESCAPE_CHARACTER
    String lineEnd = CSVWriter.DEFAULT_LINE_END

    @Override
    protected String doAggregate(Object[] fields) {
        OutputStream out = new ByteArrayOutputStream()
        out.withWriter { writer ->
            CSVWriter csvWriter = new CSVWriter(writer, separator, quotechar, escapechar, lineEnd)
            List<String> outData = []
            fields.each { outData << it.toString() }
            csvWriter.writeNext(outData.toArray(new String[0]), false)
            csvWriter.flush()
            csvWriter.close()
        }
        String result = out.toString()
        out.close()
        result
    }

}
