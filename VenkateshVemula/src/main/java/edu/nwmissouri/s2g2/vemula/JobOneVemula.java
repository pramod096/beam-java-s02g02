package edu.nwmissouri.s2g2.vemula;

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

public class JobOneVemula {

  public static void main(String[] args) {

    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline p = Pipeline.create(options);
    String dataFolder = "Filmsweb";
    
    PCollection<KV<String, String>> pcol1 = vemulaMapper(p, dataFolder, "HOLLYWOODFilm.md");

    PCollection<KV<String, String>> pcol2 = vemulaMapper(p, dataFolder, "TOLLYWOODFilm.md");

    PCollection<KV<String, String>> pcol3 = vemulaMapper(p, dataFolder, "KOLLYWOODFilm.md");

    PCollection<KV<String, String>> pcol4 = vemulaMapper(p, dataFolder, "SANDALWOODFilm.md");
    PCollection<KV<String, String>> pcol5 = vemulaMapper(p, dataFolder, "MOLLYWOODFilm.md");
    PCollection<KV<String, String>> pcol6 = vemulaMapper(p, dataFolder, "BOLLYWOODFilm.md");
    PCollectionList<KV<String, String>> pFilmsList = PCollectionList.of(pcol1).and(pcol2).and(pcol3).and(pcol4).and(pcol5).and(pcol6);

    PCollection<KV<String, String>> mList = pFilmsList.apply(Flatten.<KV<String, String>>pCollections());
    PCollection<KV<String, Iterable<String>>> utos = mList.apply(GroupByKey.<String, String>create());

    PCollection<String> pFilmLinks = utos.apply(MapElements.into(
            TypeDescriptors.strings())
            .via((mergeOut) -> mergeOut.toString()));
    pFilmLinks.apply(TextIO.write().to("VemulaOutput"));
    p.run().waitUntilFinish();
  }

  private static PCollection<KV<String, String>> vemulaMapper(Pipeline p, String dataFolder, String dataFile) {
  
    PCollection<String> pcolInputLines = p.apply(TextIO.read().from(dataFolder + "/" + dataFile));

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
