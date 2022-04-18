package edu.nwmissouri.s2g2.pramod;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptors;

public class PageRankJobOnePramod {

  public static void main(String[] args) {

    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline p = Pipeline.create(options);
    String myMiniWeb = "webBooks";
    String dataFile = "science.md";
    PCollection<KV<String, String>> pcol1 = pramodMapOne(p, myMiniWeb, dataFile);

    dataFile = "social.md";
    PCollection<KV<String, String>> pcol2 = pramodMapOne(p, myMiniWeb, dataFile);

    dataFile = "physics.md";
    PCollection<KV<String, String>> pcol3 = pramodMapOne(p, myMiniWeb, dataFile);

    dataFile = "chemistry.md";
    PCollection<KV<String, String>> pcol4 = pramodMapOne(p, myMiniWeb, dataFile);

    dataFile = "history.md";
    PCollection<KV<String, String>> pcol5 = pramodMapOne(p, myMiniWeb, dataFile);

    dataFile = "economics.md";
    PCollection<KV<String, String>> pcol6 = pramodMapOne(p, myMiniWeb, dataFile);

    dataFile = "politics.md";
    PCollection<KV<String, String>> pcol7 = pramodMapOne(p, myMiniWeb, dataFile);

    PCollectionList<KV<String, String>> pColBooksList = PCollectionList.of(pcol1).and(pcol2).and(pcol3).and(pcol4)
        .and(pcol5).and(pcol6).and(pcol7);

    PCollection<KV<String, String>> mergedList = pColBooksList.apply(Flatten.<KV<String, String>>pCollections());
    PCollection<KV<String, Iterable<String>>> urlToDocs = mergedList.apply(GroupByKey.<String, String>create());

    PCollection<String> pLinksStr = urlToDocs.apply(
        MapElements.into(
            TypeDescriptors.strings())
            .via((mergeOut) -> mergeOut.toString()));

    pLinksStr.apply(TextIO.write().to("pramodJobOneOutput"));

    p.run().waitUntilFinish();
  }

  private static PCollection<KV<String, String>> pramodMapOne(Pipeline p, String myMiniWeb, String dataFile) {
    String dataLocation = myMiniWeb + "/" + dataFile;
    PCollection<String> pcolInputLines = p.apply(TextIO.read().from(dataLocation));

    PCollection<String> pcolLinkLines = pcolInputLines.apply(Filter.by((String line) -> line.startsWith("[")));
    PCollection<String> pcolLinkPages = pcolLinkLines.apply(MapElements.into(TypeDescriptors.strings())
        .via(
            (String linkline) -> linkline.substring(linkline.indexOf("(") + 1, linkline.length() - 1)));
    PCollection<KV<String, String>> pcolKVpairs = pcolLinkPages.apply(MapElements
        .into(
            TypeDescriptors.kvs(
                TypeDescriptors.strings(), TypeDescriptors.strings()))
        .via(outlink -> KV.of(dataFile, outlink)));
    return pcolKVpairs;

  }
}