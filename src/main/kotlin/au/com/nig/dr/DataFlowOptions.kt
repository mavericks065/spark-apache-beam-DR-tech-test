package au.com.nig.dr

import org.apache.beam.sdk.extensions.gcp.options.GcpOptions
import org.apache.beam.sdk.options.Default
import org.apache.beam.sdk.options.Description
import org.apache.beam.sdk.options.Validation

interface DataFlowOptions : GcpOptions {

    @get:Description("Path to the file to read from")
    @get:Default.String("gs://my_bucket/some_file.xlsx")
    var source: String

    @get:Description("Path of the file to write to")
    @get:Validation.Required
    var output: String
}