package studentfunctions;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import studentstructures.DocumentQueryDPHScores;
import studentstructures.DocumentTermDPHScores;
import providedstructures.Query;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DocumentQueryDPHFormatter implements MapFunction<DocumentTermDPHScores, DocumentQueryDPHScores> {
    public Broadcast<List<Query>> queryList;
    public DocumentQueryDPHFormatter(Broadcast<List<Query>> queryList) {
        this.queryList = queryList;
    }

    @Override
    public DocumentQueryDPHScores call(DocumentTermDPHScores documentTermDPHScores) throws Exception {

        List<Query> queries = queryList.value();
        Map<String,Double> termScores = documentTermDPHScores.getDPHScores();
        Map<Query,Double> queryScores = new HashMap<Query,Double>();
        for(Query q:queries)//calculate DPH score for every query
        {
            double totalDPH = 0.0;
            List<String> terms = q.getQueryTerms();
            for(String term:termScores.keySet())
            {
                if(terms.contains(term)) //check if current vocabulary term is part of query
                {
                    totalDPH += termScores.get(term); //then add DPH score
                }
            }
            totalDPH /= terms.size(); //divide by size of query terms to get average DPH
            if(totalDPH > 0.0) //only add those queries for current document where dph score is non-zero
            	queryScores.put(q,totalDPH);
        }

        return new DocumentQueryDPHScores(documentTermDPHScores.getDocument(),queryScores);
    }
}
