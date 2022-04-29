package edu.nwmissouri.s2g2.vemula;

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

public class JobOneVemula {
  static Double mRValue = Double.MIN_VALUE;
  static String mRString = "";


   static class Job1Finalizer extends DoFn<KV<String, Iterable<String>>, KV<String, RankedPage>> {
    @ProcessElement
    public void processElement(@Element KV<String, Iterable<String>> element,OutputReceiver<KV<String, RankedPage>> receiver) {
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

  private static PCollection<KV<String, RankedPage>> runJob2Iteration(
      PCollection<KV<String, RankedPage>> kvReducedPairs) {

    PCollection<KV<String, RankedPage>> mappedKVs = kvReducedPairs.apply(ParDo.of(new Job2Mapper()));
    PCollection<KV<String, Iterable<RankedPage>>> reducedKVs = mappedKVs.apply(GroupByKey.<String, RankedPage>create());
    PCollection<KV<String, RankedPage>> updatedOutput = reducedKVs.apply(ParDo.of(new Job2Updater()));
    return updatedOutput;
  }


static class Job2Mapper extends DoFn<KV<String, RankedPage>, KV<String, RankedPage>> {
    @ProcessElement
    public void processElement(@Element KV<String, RankedPage> element,
        OutputReceiver<KV<String, RankedPage>> receiver) {
      Integer votes = 0;
      String pageName;
      Double pageRank;

      ArrayList<VotingPage> voters = element.getValue().getVoters();
      if (voters instanceof Collection) {
        votes = ((Collection<VotingPage>) voters).size();
      }
      String contributingPageName = element.getKey();

      Double contributingPageRank = element.getValue().getRank();
      for (VotingPage votingPage : voters) {
        pageName = votingPage.getname();
        pageRank = votingPage.getRank();
        VotingPage contributor = new VotingPage(contributingPageName, contributingPageRank, votes);
        ArrayList<VotingPage> votingpagearr = new ArrayList<VotingPage>();
        votingpagearr.add(contributor);
        receiver.output(KV.of(votingPage.getname(), new RankedPage(pageName, pageRank, votingpagearr)));

      }
    }
  }
static class Job2Updater extends DoFn<KV<String, Iterable<RankedPage>>, KV<String, RankedPage>> {
    @ProcessElement
    public void processElement(@Element KV<String, Iterable<RankedPage>> element,
        OutputReceiver<KV<String, RankedPage>> receiver) {
      Double dampingFactor = 0.85;
      Double updatedRank = (1 - dampingFactor);
      ArrayList<VotingPage> voternew = new ArrayList<VotingPage>();
      for (RankedPage page : element.getValue()) {
        if (page != null) {
          for (VotingPage vp : page.getVoters()) {
            voternew.add(vp);
            updatedRank += (dampingFactor) * vp.getRank() / (double) vp.getvote();
         }
         }
      }
      receiver.output(KV.of(element.getKey(), new RankedPage(element.getKey(), updatedRank, voternew)));

    }
  }
  static class Job3Finalizer extends DoFn<KV<String, RankedPage>, KV<Double, String>> {
    @ProcessElement
    public void processElement(@Element KV<String, RankedPage> element,
        OutputReceiver<KV<Double, String>> receiver) {
      RankedPage rankpage = element.getValue();
      if (mRValue < rankpage.getRank()) {
        mRString = element.getKey();
        mRValue = rankpage.getRank();
      }
    receiver.output(KV.of(rankpage.getRank(), element.getKey()));
    }
  }


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
    PCollection<KV<String, String>> pcol7 = vemulaMapper(p, dataFolder, "Film.md");
    PCollectionList<KV<String, String>> pFilmsList = PCollectionList.of(pcol1).and(pcol2).and(pcol3).and(pcol4).and(pcol5).and(pcol6);

    PCollection<KV<String, String>> mList = pFilmsList.apply(Flatten.<KV<String, String>>pCollections());
    PCollection<KV<String, Iterable<String>>> utos = mList.apply(GroupByKey.<String, String>create());
PCollection<KV<String, RankedPage>> job2in = utos.apply(ParDo.of(new Job1Finalizer()));

    PCollection<KV<String, RankedPage>> job2out = null;

    int iterations = 50;
    for (int i = 1; i <= iterations; i++) {
      job2out = runJob2Iteration(job2in);job2in = job2out;
    }
    PCollection<KV<Double, String>> job3output = job2out.apply(ParDo.of(new Job3Finalizer()));
    PCollection<KV<Double, String>> finalJob3MaxOutput = job3output.apply(Filter.by((KV<Double, String> element) -> {
         return element.getValue().equals(mRString);
       
    }));
    PCollection<String> Vemulafinaloutput = finalJob3MaxOutput.apply(MapElements.into(
        TypeDescriptors.strings())
        .via(kv -> kv.toString()));

    Vemulafinaloutput.apply(TextIO.write().to("VemulaOutput"));

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

// Acknowledgement : I refered the code form the saikiran, pramod and ramu to complete the Jobs.
// Saikiran helped me a lot to implement the Custom page and the remaining. 