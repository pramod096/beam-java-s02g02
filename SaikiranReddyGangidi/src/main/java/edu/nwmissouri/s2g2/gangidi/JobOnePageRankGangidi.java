package edu.nwmissouri.s2g2.gangidi;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

public class JobOnePageRankGangidi {

  static String maxRankString = "";
  static Double maxRankValue = Double.MIN_VALUE;

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
  private static PCollection<KV<String, RankedPage>> runJob2Iteration(
      PCollection<KV<String, RankedPage>> kvReducedPairs) {

    PCollection<KV<String, RankedPage>> mappedKVs = kvReducedPairs.apply(ParDo.of(new Job2Mapper()));

    // KV{README.md, README.md, 1.00000, 0, [java.md, 1.00000,1]}
    // KV{README.md, README.md, 1.00000, 0, [go.md, 1.00000,1]}
    // KV{java.md, java.md, 1.00000, 0, [README.md, 1.00000,3]}

    PCollection<KV<String, Iterable<RankedPage>>> reducedKVs = mappedKVs
        .apply(GroupByKey.<String, RankedPage>create());

    // KV{java.md, [java.md, 1.00000, 0, [README.md, 1.00000,3]]}
    // KV{README.md, [README.md, 1.00000, 0, [python.md, 1.00000,1], README.md,
    // 1.00000, 0, [java.md, 1.00000,1], README.md, 1.00000, 0, [go.md, 1.00000,1]]}

    PCollection<KV<String, RankedPage>> updatedOutput = reducedKVs.apply(ParDo.of(new Job2Updater()));

    // KV{README.md, README.md, 2.70000, 0, [java.md, 1.00000,1, go.md, 1.00000,1,
    // python.md, 1.00000,1]}
    // KV{python.md, python.md, 0.43333, 0, [README.md, 1.00000,3]}
    return updatedOutput;
  }

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

  static class Job2Mapper extends DoFn<KV<String, RankedPage>, KV<String, RankedPage>> {
    @ProcessElement
    public void processElement(@Element KV<String, RankedPage> element,
        OutputReceiver<KV<String, RankedPage>> receiver) {
      Integer votes = 0;
      ArrayList<VotingPage> voters = element.getValue().getVoters();
      if (voters instanceof Collection) {
        votes = ((Collection<VotingPage>) voters).size();
      }
      String contributingPageName = element.getKey();

      Double contributingPageRank = element.getValue().getRank();
      String pName;
      Double pRank;
      for (VotingPage votingPage : voters) {
        pName = votingPage.getVotername();
        pRank = votingPage.getRank();

        VotingPage contributor = new VotingPage(contributingPageName, contributingPageRank, votes);
        ArrayList<VotingPage> vparr = new ArrayList<VotingPage>();
        vparr.add(contributor);
        receiver.output(KV.of(votingPage.getVotername(), new RankedPage(pName, pRank, vparr)));

      }
    }
  }

  static class Job2Updater extends DoFn<KV<String, Iterable<RankedPage>>, KV<String, RankedPage>> {
    @ProcessElement
    public void processElement(@Element KV<String, Iterable<RankedPage>> element,
        OutputReceiver<KV<String, RankedPage>> receiver) {
      Double dampingFactor = 0.85;
      Double updatedRank = (1 - dampingFactor);
      ArrayList<VotingPage> newVoters = new ArrayList<VotingPage>();
      for (RankedPage pg : element.getValue()) {
        if (pg != null) {
          for (VotingPage vp : pg.getVoters()) {
            newVoters.add(vp);

            updatedRank += (dampingFactor) * vp.getRank() / (double) vp.getContributorVotes();

          }

        }
      }
      receiver.output(KV.of(element.getKey(), new RankedPage(element.getKey(), updatedRank, newVoters)));

    }
  }

  static class Job3Finalizer extends DoFn<KV<String, RankedPage>, KV<Double, String>> {
    @ProcessElement
    public void processElement(@Element KV<String, RankedPage> element,
        OutputReceiver<KV<Double, String>> receiver) {
      RankedPage rp = element.getValue();
      if (maxRankValue < rp.getRank()) {
        maxRankString = element.getKey();
        maxRankValue = rp.getRank();
      }

      receiver.output(KV.of(rp.getRank(), element.getKey()));
    }
  }

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
    PCollection<KV<String, Iterable<String>>> urlToStringList = mergedList.apply(GroupByKey.<String, String>create());

    PCollection<KV<String, RankedPage>> job2in = urlToStringList.apply(ParDo.of(new Job1Finalizer()));

    PCollection<KV<String, RankedPage>> job2out = null;

    int iterations = 50;
    for (int i = 1; i <= iterations; i++) {
      job2out = runJob2Iteration(job2in);
      job2in = job2out;

    }

    PCollection<KV<Double, String>> job3output = job2out.apply(ParDo.of(new Job3Finalizer()));

    PCollection<KV<Double, String>> finalJob3MaxOutput = job3output.apply(Filter.by((KV<Double, String> element) -> {

      if (element.getValue().equals(maxRankString)) {
        return true;
      } else {
        return false;
      }
    }));

    PCollection<String> finaloutput = finalJob3MaxOutput.apply(MapElements.into(
        TypeDescriptors.strings())
        .via(kv -> kv.toString()));

    finaloutput.apply(TextIO.write().to("GangidifinalMaxOutput"));

    PCollection<String> outputjob3comp = job3output.apply(MapElements.into(
        TypeDescriptors.strings())
        .via(kv -> kv.toString()));

    outputjob3comp.apply(TextIO.write().to("GangidiJobThreeoutput"));

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
