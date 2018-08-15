package au.com.nig.dr

import au.com.nig.dr.csv.ExtractFileData
import au.com.nig.dr.util.BigQueryUtil.formatBigQueryRow
import au.com.nig.dr.util.BigQueryUtil.getTableSchema
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult
import org.apache.beam.sdk.io.tika.TikaIO
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.ParDo

object Main {

    @JvmStatic
    fun main(args: Array<String>) {
        val options = PipelineOptionsFactory.fromArgs(*args).withValidation()
                .`as`(DataFlowOptions::class.java)
        val p = Pipeline.create(options)

        // get xlsx from gcp
        val files = p.apply("Reading excel source", TikaIO.parse().filepattern(options.source))

        // parse source
        files.apply("Parsing excel data", ParDo.of(ExtractFileData()))
                // write it to bigquery
                .apply<WriteResult>("Writing data to BigQuery", BigQueryIO.write<List<String>>()
                        .to(options.project + ":" + options.output)
                        .withSchema(getTableSchema())
                        .withFormatFunction({ formatBigQueryRow(it) })
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE))

        p.run().waitUntilFinish()
    }

}