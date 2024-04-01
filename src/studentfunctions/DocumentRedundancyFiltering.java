package studentfunctions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.api.java.function.ReduceFunction;

import providedstructures.DocumentRanking;
import providedstructures.RankedResult;
import providedutilities.TextDistanceCalculator;

public class DocumentRedundancyFiltering implements ReduceFunction<DocumentRanking>{

	@Override
	public DocumentRanking call(DocumentRanking dr1, DocumentRanking dr2) throws Exception {
		List<RankedResult> rankedResultList = dr1.getResults();
		rankedResultList.addAll(dr2.getResults());
		Collections.sort(rankedResultList);
		Collections.reverse(rankedResultList);
		List<RankedResult> result = new ArrayList<>();
		Set<RankedResult> removedDoc = new HashSet<>();
		//calculating text distance between every pair of documents
		for (int i = 0; i < rankedResultList.size(); ++i) {
			RankedResult maxRankedResult = rankedResultList.get(i);
			if (removedDoc.contains(maxRankedResult))
				continue;
			for (int j = 0; j < rankedResultList.size(); ++j) {
				RankedResult rankedResult = rankedResultList.get(j);
				if (removedDoc.contains(rankedResult))
					continue;
				if (TextDistanceCalculator.similarity(maxRankedResult.getArticle().getTitle(), rankedResult.getArticle().getTitle()) < 0.5) {
					if (rankedResult.getScore() > maxRankedResult.getScore()) {
						removedDoc.add(maxRankedResult);
						maxRankedResult = rankedResult;
					} else {
						removedDoc.add(rankedResult);
					}
				}
			}
			result.add(maxRankedResult);
		}
		//sorting the non-redundant documents in descending order
		Collections.sort(result);
		Collections.reverse(result);
		return new DocumentRanking(dr1.getQuery(), result.subList(0, (int) Math.min(10, result.size())));//returning a final list of documents capped at size 10
	}

}
