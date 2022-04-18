package edu.nwmissouri.s2g2.gangidi;

import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

public class JobOnePageRankGangidi {

  public static void main(String[] args) {

    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline p = Pipeline.create(options);
    String dataFolder = "cricketweb";
    String dataFile = "cricket.md";
    PCollection<KV<String, String>> pcol1 = gangidiMapper(p, dataFolder, dataFile);

    dataFile = "worldcup.md";
    PCollection<KV<String, String>> pcol2 = gangidiMapper(p, dataFolder, dataFile);

    dataFile = "ODIcricket.md";
    PCollection<KV<String, String>> pcol3 = gangidiMapper(p, dataFolder, dataFile);

    dataFile = "T20cricket.md";
    PCollection<KV<String, String>> pcol4 = gangidiMapper(p, dataFolder, dataFile);

    dataFile = "TESTcricket.md";
    PCollection<KV<String, String>> pcol5 = gangidiMapper(p, dataFolder, dataFile);

    dataFile = "IPLcricket.md";
    PCollection<KV<String, String>> pcol6 = gangidiMapper(p, dataFolder, dataFile);

    PCollectionList<KV<String, String>> pcolCricketList = PCollectionList.of(pcol1).and(pcol2).and(pcol3).and(pcol4)
        .and(pcol5).and(pcol6);

    PCollection<KV<String, String>> mergedList = pcolCricketList.apply(Flatten.<KV<String, String>>pCollections());
    PCollection<KV<String, Iterable<String>>> urlToDocs = mergedList.apply(GroupByKey.<String, String>create());

    PCollection<String> pLinksStr = urlToDocs.apply(
        MapElements.into(
            TypeDescriptors.strings())
            .via((mergeOut) -> mergeOut.toString()));

    pLinksStr.apply(TextIO.write().to("gangidijoboneout"));

    p.run().waitUntilFinish();
  }

  private static PCollection<KV<String, String>> gangidiMapper(Pipeline p, String dataFolder, String dataFile) {
    String dataLocation = dataFolder + "/" + dataFile;
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
