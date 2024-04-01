package studentfunctions;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import providedutilities.DPHScorer;
import studentstructures.DocumentTermDPHScores;
import studentstructures.*;


public class DocumentTermDPHFormatter implements MapFunction<Document,DocumentTermDPHScores>{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -921565245776923508L;
	Broadcast<List<String>> queryVocab;
	Broadcast<Long> numDocuments;
	Broadcast<Map<String,Integer>> termCountsInCorpus;
	Broadcast<Double> averageDocLength;
	
	public DocumentTermDPHFormatter(Broadcast<List<String>> queryVocab,Broadcast<Long> numDocuments,Broadcast<Map<String,Integer>> termCountsInCorpus,Broadcast<Double> averageDocLength) {
		super();
		this.queryVocab = queryVocab;
		this.numDocuments = numDocuments;
		this.termCountsInCorpus = termCountsInCorpus;
		this.averageDocLength = averageDocLength;
	}
	@Override
	public DocumentTermDPHScores call(Document document) throws Exception {
		//get broadcast variable values
		Set<String> vocabulary = new HashSet<>(queryVocab.value());//get unique query vocabulary
		long n_docs = numDocuments.value();
		Map<String,Integer> termFreqInCorpus = termCountsInCorpus.value();
		double avDocLength = averageDocLength.value();
		
		Map<String,Double> termDPHmapping = new HashMap<String,Double>();
		
		for(String term: vocabulary)//calculate DPH for every vocabulary term and current document
		{
			DPHScorer scorer = new DPHScorer();
			
			int currDocLength = 0;
			for(short values:document.getContentTermCountMap().values())//get current document length
				currDocLength += (int)values;
			
			double score = scorer.getDPHScore(document.getContentTermCountMap().getOrDefault(term,(short)0),termFreqInCorpus.getOrDefault(term,0) , currDocLength, avDocLength, n_docs);
			if(!Double.isNaN(score))//if the DPH score is not NaN, then only add it to list
			{
				termDPHmapping.put(term, score);
			}
		}
		
		return new DocumentTermDPHScores(document,termDPHmapping);
	}
	
}
