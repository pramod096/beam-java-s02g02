package edu.nwmissouri.s2g2.pramod;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.lang.Double;
import java.lang.Integer;
import java.io.Serializable;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptors;

public class PageRankJobOnePramod {

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

  /*
   * Gives the RankedPage Object for all the pages having incoming links from
   * other pages.
   */
  static class Job2Mapper extends DoFn<KV<String, RankedPage>, KV<String, RankedPage>> {
    @ProcessElement
    public void processElement(@Element KV<String, RankedPage> element,
        OutputReceiver<KV<String, RankedPage>> receiver) {
      Integer votes = 0;
      ArrayList<VotingPage> voters = element.getValue().getPagesVoted();
      if (voters instanceof Collection) {
        votes = ((Collection<VotingPage>) voters).size();
      }

      for (VotingPage vp : voters) {
        String pageName = vp.getName();
        Double pageRank = vp.getRank();
        String contributingPageName = element.getKey();
        Double contributingPageRank = element.getValue().getRankValue();
        VotingPage contributor = new VotingPage(contributingPageName, contributingPageRank, votes);
        ArrayList<VotingPage> arr = new ArrayList<>();
        arr.add(contributor);
        receiver.output(KV.of(vp.getName(), new RankedPage(pageName, pageRank, arr)));
      }
    }
  }

  /*
   * Calculates the pages rank value of each page based.
   */
  static class Job2Updater extends DoFn<KV<String, Iterable<RankedPage>>, KV<String, RankedPage>> {
    @ProcessElement
    public void processElement(@Element KV<String, Iterable<RankedPage>> element,
        OutputReceiver<KV<String, RankedPage>> receiver) {
      String thisPage = element.getKey();
      Iterable<RankedPage> rankedPages = element.getValue();
      Double dampingFactor = 0.85;
      Double updatedRank = (1 - dampingFactor);
      ArrayList<VotingPage> newVoters = new ArrayList<VotingPage>();

      for (RankedPage pg : rankedPages) {
        if (pg != null) {
          for (VotingPage vp : pg.getPagesVoted()) {
            newVoters.add(vp);
            updatedRank += (dampingFactor) * vp.getRank() / (double) vp.getVotes();
          }
        }
      }

      receiver.output(KV.of(thisPage, new RankedPage(thisPage, updatedRank, newVoters)));
    }
  }

  /**
   * Gets KV pairs which contain the final page rank value of each page.
   */
  static class Job3Finder extends DoFn<KV<String, RankedPage>, KV<String, Double>> {
    @ProcessElement
    public void processElement(@Element KV<String, RankedPage> element,
        OutputReceiver<KV<String, Double>> receiver) {
      String currentPage = element.getKey();
      Double currentPageRank = element.getValue().getRankValue();

      receiver.output(KV.of(currentPage, currentPageRank));
    }
  }

  /**
   * Class which implements the Comparator interface to get the page with highest
   * page rank value.
   */
  public static class Job3Final implements Comparator<KV<String, Double>>, Serializable {
    @Override
    public int compare(KV<String, Double> o1, KV<String, Double> o2) {
      return o1.getValue().compareTo(o2.getValue());
    }
  }

  /**
   * Method to read each line from a web page and find the outgoing links.
   * 
   * @param p         Pipeline.
   * @param myMiniWeb Location containing all the web pages.
   * @param dataFile  The exact web page to read.
   */
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

  /**
   * Main Method
   * 
   * @param args
   */
  public static void main(String[] args) {

    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline p = Pipeline.create(options);

    // Name of my mini Internet
    String myMiniWeb = "webBooks";

    // Varibale to store all the web pages and pass as input to pramodMapOne Method.
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
    PCollection<KV<String, Iterable<String>>> kvStringReducedPairs = mergedList
        .apply(GroupByKey.<String, String>create());

    // PCollection to Store Job1 Output
    PCollection<KV<String, RankedPage>> job2in = kvStringReducedPairs.apply(ParDo.of(new Job1Finalizer()));

    PCollection<KV<String, RankedPage>> job2out = null;

    PCollection<KV<String, RankedPage>> mappedKVs = null;

    PCollection<KV<String, Iterable<RankedPage>>> reducedKVs = null;

    int iterations = 50;

    // Looping a fixed number of times to update the pagerank of each web page.
    for (int i = 1; i <= iterations; i++) {
      mappedKVs = job2in.apply(ParDo.of(new Job2Mapper()));

      reducedKVs = mappedKVs
          .apply(GroupByKey.<String, RankedPage>create());

      job2out = reducedKVs.apply(ParDo.of(new Job2Updater()));

      job2in = job2out;

    }

    // PCollection to find and store all the web pages final page rank value using
    // Job3Finder, as KV Pairs.
    PCollection<KV<String, Double>> maxRank = job2out.apply(ParDo.of(new Job3Finder()));

    // Finding the Page with Highest PageRank Value using Job3Final.
    PCollection<KV<String, Double>> finalMax = maxRank.apply(Combine.globally(Max.of(new Job3Final())));

    // Printing the final output to a text file.
    PCollection<String> fnl = finalMax.apply(MapElements.into(
        TypeDescriptors.strings())
        .via(kv -> kv.toString()));
    fnl.apply(TextIO.write().to("finalOutputPramod"));

    p.run().waitUntilFinish();
  }
}