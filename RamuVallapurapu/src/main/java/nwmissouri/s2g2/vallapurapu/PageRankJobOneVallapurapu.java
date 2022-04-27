package nwmissouri.s2g2.vallapurapu;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;

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

public class PageRankJobOneVallapurapu {

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
			ArrayList<VotingPage> voters = element.getValue().getPagesVoted();
			if (voters instanceof Collection) {
				votes = ((Collection<VotingPage>) voters).size();
			}

			for (VotingPage votingPage : voters) {
				String pageName = votingPage.getName();
				Double pageRank = votingPage.getRank();
				String contributingPageName = element.getKey();
				Double contributingPageRank = element.getValue().getRankValue();
				VotingPage contributor = new VotingPage(contributingPageName, contributingPageRank, votes);
				ArrayList<VotingPage> arr = new ArrayList<>();
				arr.add(contributor);
				receiver.output(KV.of(votingPage.getName(), new RankedPage(pageName, pageRank, arr)));
			}
		}

	}

	static class Job2Updater extends DoFn<KV<String, Iterable<RankedPage>>, KV<String, RankedPage>> {

		@ProcessElement
		public void processElement(@Element KV<String, Iterable<RankedPage>> element,
				OutputReceiver<KV<String, RankedPage>> receiver) {
			String thisPage = element.getKey();
			Iterable<RankedPage> rankedPages = element.getValue();
			Double dampingFactor = 0.85;
			Double updatedRank = (1 - dampingFactor);
			ArrayList<VotingPage> newVoters = new ArrayList<VotingPage>();

			for (RankedPage rankedPage : rankedPages) {
				if (rankedPage != null) {
					for (VotingPage votePage : rankedPage.getPagesVoted()) {
						newVoters.add(votePage);
						updatedRank += (dampingFactor) * votePage.getRank() / (double) votePage.getVotes();
					}
				}
			}

			receiver.output(KV.of(thisPage, new RankedPage(thisPage, updatedRank, newVoters)));
		}
	}

	static class Job3Finder extends DoFn<KV<String, RankedPage>, KV<String, Double>> {
		@ProcessElement
		public void processElement(@Element KV<String, RankedPage> element,
				OutputReceiver<KV<String, Double>> receiver) {
			String currentPage = element.getKey();
			Double currentPageRank = element.getValue().getRankValue();

			receiver.output(KV.of(currentPage, currentPageRank));
		}
	}

	public static class Job3Final implements Comparator<KV<String, Double>>, Serializable {
		@Override
		public int compare(KV<String, Double> o1, KV<String, Double> o2) {
			return o1.getValue().compareTo(o2.getValue());
		}
	}

	public static void main(String[] args) {

		PipelineOptions options = PipelineOptionsFactory.create();
		Pipeline p = Pipeline.create(options);
		String myMiniWeb = "webMedia";
		String dataFile = "facebook.md";
		PCollection<KV<String, String>> pcol1 = vallapurapuMapOne(p, myMiniWeb, dataFile);

		dataFile = "instagram.md";
		PCollection<KV<String, String>> pcol2 = vallapurapuMapOne(p, myMiniWeb, dataFile);

		dataFile = "sanpchat.md";
		PCollection<KV<String, String>> pcol3 = vallapurapuMapOne(p, myMiniWeb, dataFile);

		dataFile = "twitter.md";
		PCollection<KV<String, String>> pcol4 = vallapurapuMapOne(p, myMiniWeb, dataFile);

		PCollectionList<KV<String, String>> pColBooksList = PCollectionList.of(pcol1).and(pcol2).and(pcol3).and(pcol4);

		PCollection<KV<String, String>> mergedList = pColBooksList.apply(Flatten.<KV<String, String>>pCollections());
		PCollection<KV<String, Iterable<String>>> urlToDocs = mergedList.apply(GroupByKey.<String, String>create());

		PCollection<String> pLinksStr = urlToDocs
				.apply(MapElements.into(TypeDescriptors.strings()).via((mergeOut) -> mergeOut.toString()));

		pLinksStr.apply(TextIO.write().to("vallapurapuJobOneOutput"));

		p.run().waitUntilFinish();
	}

	private static PCollection<KV<String, String>> vallapurapuMapOne(Pipeline p, String myMiniWeb, String dataFile) {
		String dataLocation = myMiniWeb + "/" + dataFile;
		PCollection<String> pcolInputLines = p.apply(TextIO.read().from(dataLocation));

		PCollection<String> pcolLinkLines = pcolInputLines.apply(Filter.by((String line) -> line.startsWith("[")));
		PCollection<String> pcolLinkPages = pcolLinkLines.apply(MapElements.into(TypeDescriptors.strings())
				.via((String linkline) -> linkline.substring(linkline.indexOf("(") + 1, linkline.length() - 1)));
		PCollection<KV<String, String>> pcolKVpairs = pcolLinkPages
				.apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
						.via(outlink -> KV.of(dataFile, outlink)));
		return pcolKVpairs;

	}
}