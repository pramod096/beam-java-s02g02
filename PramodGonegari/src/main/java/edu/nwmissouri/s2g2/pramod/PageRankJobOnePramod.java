package edu.nwmissouri.s2g2.pramod;

import java.util.ArrayList;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.yaml.snakeyaml.nodes.CollectionNode;

public class PageRankJobOnePramod {

  // DEFINE DOFNS
  // ==================================================================
  // You can make your pipeline assembly code less verbose by defining
  // your DoFns statically out-of-line.
  // Each DoFn<InputT, OutputT> takes previous output
  // as input of type InputT
  // and transforms it to OutputT.
  // We pass this DoFn to a ParDo in our pipeline.

  /**
   * DoFn Job1Finalizer takes KV(String, String List of outlinks) and transforms
   * the value into our custom RankedPage Value holding the page's rank and list
   * of voters.
   * 
   * The output of the Job1 Finalizer creates the initial input into our
   * iterative Job 2.
   */
  static class Job1Finalizer extends DoFn<KV<String, Iterable<String>>, KV<String, RankedPage>> {
    @ProcessElement
    public void processElement(@Element KV<String, Iterable<String>> element,
        OutputReceiver<KV<String, RankedPage>> receiver) {
      Integer contributorVotes = 0;
      if (element.getValue() instanceof Collection) {
        contributorVotes = ((Collection<String>) element.getValue()).size();
      }
      ArrayList<VotingPage> voters = new ArrayList<VotingPage>();
      for (String voterName : element.getValue()) {
        if (!voterName.isEmpty()) {
          voters.add(new VotingPage(voterName, contributorVotes));
        }
      }
      receiver.output(KV.of(element.getKey(), new RankedPage(element.getKey(), voters)));
    }
  }

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