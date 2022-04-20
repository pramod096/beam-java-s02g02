package edu.nwmissouri.s2g2.gunturu;

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

public class PageRankJobOneGunturu {

  public static void main(String[] args) {

    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline p = Pipeline.create(options);
    String vechicleweb = "vechiclesweb";
  
    PCollection<KV<String, String>> parallelCol1 = narendraMapOne(p, vechicleweb, "access125.md");

    PCollection<KV<String, String>> parallelCol2 = narendraMapOne(p, vechicleweb, "elantra.md");

    PCollection<KV<String, String>> parallelCol3 = narendraMapOne(p, vechicleweb, "fourwheeler.md");

    PCollection<KV<String, String>> parallelCol4 = narendraMapOne(p, vechicleweb, "hyundai.md");

    PCollection<KV<String, String>> parallelCol5 = narendraMapOne(p, vechicleweb, "maruthi.md");

    PCollection<KV<String, String>> parallelCol6 = narendraMapOne(p, vechicleweb, "twowheelers.md");


    PCollectionList<KV<String, String>> parallelColVehicleList = PCollectionList.of(parallelCol1).and(parallelCol2).and(parallelCol3).and(parallelCol4)
        .and(parallelCol5).and(parallelCol6);

    PCollection<KV<String, String>> merList = parallelColVehicleList.apply(Flatten.<KV<String, String>>pCollections());
    PCollection<KV<String, Iterable<String>>> urlToDocs = merList.apply(GroupByKey.<String, String>create());

    PCollection<String> parallelLinksStr = urlToDocs.apply(
        MapElements.into(
            TypeDescriptors.strings())
            .via((mergeOut) -> mergeOut.toString()));

    parallelLinksStr.apply(TextIO.write().to("narendraOutputJobOne"));

    p.run().waitUntilFinish();
  }

  private static PCollection<KV<String, String>> narendraMapOne(Pipeline p, String myMiniWeb, String dataFile) {
    String dataLocation = myMiniWeb + "/" + dataFile;
    PCollection<String> parallelColInputLines = p.apply(TextIO.read().from(dataLocation));

    PCollection<String> parallelColLinkLines = parallelColInputLines.apply(Filter.by((String line) -> line.startsWith("[")));
    PCollection<String> parallelColLinkPages = parallelColLinkLines.apply(MapElements.into(TypeDescriptors.strings())
        .via(
            (String linkline) -> linkline.substring(linkline.indexOf("(") + 1, linkline.length() - 1)));
    PCollection<KV<String, String>> parallelColKVpairs = parallelColLinkPages.apply(MapElements
        .into(
            TypeDescriptors.kvs(
                TypeDescriptors.strings(), TypeDescriptors.strings()))
        .via(outlink -> KV.of(dataFile, outlink)));
    return parallelColKVpairs;

  }
}