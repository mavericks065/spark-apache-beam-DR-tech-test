package au.com.nig.dr.csv;


import org.apache.beam.sdk.io.tika.ParseResult;
import org.apache.beam.sdk.transforms.DoFn;

import java.util.Arrays;
import java.util.List;

public class ExtractFileData extends DoFn<ParseResult, List<String>> {

    @ProcessElement
    public void processElement(@Element ParseResult element, OutputReceiver<List<String>> receiver) {
        Arrays.stream(element.getContent().trim().split("\n"))
                .forEach(row -> receiver.output(Arrays.asList(row.trim().split("\t"))));
    }

}