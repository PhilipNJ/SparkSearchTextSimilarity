package studentfunctions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.function.FlatMapFunction;

import providedstructures.DocumentRanking;
import providedstructures.Query;
import providedstructures.RankedResult;
import studentstructures.Document;
import studentstructures.DocumentQueryDPHScores;

public class DocumentRankingFormatter implements FlatMapFunction<DocumentQueryDPHScores,DocumentRanking>{

	@Override
	public Iterator<DocumentRanking> call(DocumentQueryDPHScores t) throws Exception {
		List<DocumentRanking> result = new ArrayList<>();
		for (Map.Entry<Query, Double> e : t.getDPHScores().entrySet()) {
			Query query = e.getKey();
			Double dphScore = e.getValue();

			Document doc = t.getDocument();
			RankedResult rankedResult = new RankedResult(doc.getDocId(), doc.getArticle(), dphScore);

			DocumentRanking documentRanking = new DocumentRanking(query, Arrays.asList(rankedResult));
			result.add(documentRanking);
		}

		return result.iterator();
	}

}
