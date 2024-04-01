package apps;

import java.io.File;
import java.util.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;
import providedfunctions.NewsFormaterMap;
import providedfunctions.QueryFormaterMap;
import providedstructures.DocumentRanking;
import providedstructures.NewsArticle;
import providedstructures.Query;
import studentfunctions.*;


import studentstructures.Document;
import studentstructures.DocumentQueryDPHScores;
import studentstructures.DocumentTermDPHScores;


/** 
 * By default, running this class will execute the topology defined in the
 * rankDocuments() method in local mode, although this may be overridden by
 * the spark.master environment variable.
 */
public class AssessedExercise {

	
	public static void main(String[] args) {
		
		File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file so we can get an absolute path for it
		System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that Spark finds it
		
		// The code submitted for the assessed exercise may be run in either local or remote modes
		// Configuration of this will be performed based on an environment variable
		String sparkMasterDef = System.getenv("spark.master");
		if (sparkMasterDef==null) sparkMasterDef = "local[4]"; // default is local mode with two executors
		
		String sparkSessionName = "BigDataAE"; // give the session a name
		
		// Create the Spark Configuration 
		SparkConf conf = new SparkConf()
				.setMaster(sparkMasterDef)
				.setAppName(sparkSessionName);
		
		// Create the spark session
		SparkSession spark = SparkSession
				  .builder()
				  .config(conf)
				  .getOrCreate();
	
		
		// Get the location of the input queries
		String queryFile = System.getenv("bigdata.queries");
		if (queryFile==null) queryFile = "data/queries.list"; // default is a sample with 3 queries
		
		// Get the location of the input news articles
		String newsFile = System.getenv("bigdata.news");
		if (newsFile==null) newsFile = "data/TREC_Washington_Post_collection.v2.jl.fix.json"; // default is a sample of 5000 news articles
		
		// Call the student's code
		List<DocumentRanking> results = rankDocuments(spark, queryFile, newsFile);
		
		// Close the spark session
		spark.close();
		
		// Check if the code returned any results
		if (results==null) System.err.println("Topology return no rankings, student code may not be implemented, skiping final write.");
		else {
			
			// We have set of output rankings, lets write to disk
			
			// Create a new folder 
			File outDirectory = new File("results/"+System.currentTimeMillis());
			if (!outDirectory.exists()) outDirectory.mkdir();
			
			// Write the ranking for each query as a new file
			for (DocumentRanking rankingForQuery : results) {
				rankingForQuery.write(outDirectory.getAbsolutePath());
			}
		}
		
		
	}
	
	
	
	public static List<DocumentRanking> rankDocuments(SparkSession spark, String queryFile, String newsFile) {
		
		// Load queries and news articles
		Dataset<Row> queriesjson = spark.read().text(queryFile);
		Dataset<Row> newsjson = spark.read().text(newsFile); // read in files as string rows, one row per article
		
		// Perform an initial conversion from Dataset<Row> to Query and NewsArticle Java objects
		Dataset<Query> queries = queriesjson.map(new QueryFormaterMap(), Encoders.bean(Query.class)); // this converts each row into a Query
		Dataset<NewsArticle> news = newsjson.map(new NewsFormaterMap(), Encoders.bean(NewsArticle.class)); // this converts each row into a NewsArticle

		//Filter out news article with null title
		Dataset<NewsArticle> filteredNews = news.filter((FilterFunction<NewsArticle>) n -> {
					return n.getTitle() != null;
				});
				
		// Create Document from News Article - comprises of news articles with their content tokenized along with token counts
		Dataset<Document> document = filteredNews.map(new NewsArticleTokenizer(), Encoders.bean(Document.class));
				
		long numDocs = document.count(); //no. of docs
				
		// Create terms from Query - essentially the vocabulary of the entire query list
		Dataset<String> terms = queries.flatMap((FlatMapFunction<Query, String>) q -> {
					List<String> listOfQueryTerms = q.getQueryTerms();
					return listOfQueryTerms.iterator();
				}, Encoders.STRING());

				
		/****Pipeline to obtain term counts in entire corpus of documents****/
		//Flatmap gets terms and their corresponding counts for every document as a Tuple2<String,Integer>
		Dataset<Tuple2<String,Integer>> termCounts = document.flatMap(new DocumentTermCountFormatter(), Encoders.tuple(Encoders.STRING(), Encoders.INT()));
		//It is then grouped by every unique term across the corpus
		KeyValueGroupedDataset<String,Tuple2<String,Integer>> termCountsGrouped = termCounts.groupByKey(new GetTermCount(), Encoders.STRING());
		//MapGroups does a reduction on every group essentially adding up the counts for each term
		Dataset<Tuple2<String,Integer>> termsAndCounts = termCountsGrouped.mapGroups(new TermGroupsFormatter(),Encoders.tuple(Encoders.STRING(), Encoders.INT()));
				
		Map<String,Integer> termsByCounts= new HashMap<String,Integer>(); //term counts for entire corpus - needed for DPH
		for(Tuple2<String,Integer> tuple: termsAndCounts.collectAsList()) termsByCounts.put(tuple._1, tuple._2);
		//Reduce function adds up every (Term,Frequency) pair essentially giving a single Tuple2<String,Integer>
		Tuple2<String, Integer> sumOfAllTerms = termCounts.reduce(new TermSum());
		//the second element of the tuple is the count of all terms in the entire corpus
		double averageDocumentLength = sumOfAllTerms._2/(double)numDocs; //get average document length
				
				
		/*****Necessary broadcast variables*****/
		//needed to check if query terms is in document
		Broadcast<List<Query>> queryList = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(queries.collectAsList()); //list of queries
		//needed for DPH calculation
		Broadcast<List<String>> broadcastTerms = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(terms.collectAsList()); //query vocabulary
		Broadcast<Long> broadcastNumDocuments = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(numDocs); // no. of docs
		Broadcast<Map<String,Integer>> broadcastTermCountsInCorpus = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(termsByCounts);// term counts for entire corpus
		Broadcast<Double> broadcastAverageDocumentLength = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(averageDocumentLength); //average document length
				
		//Maps document to a dataset comprising of every document and a hashmap of every query term with their DPH score for that document
		Dataset<DocumentTermDPHScores> documentTermDPHDataset = document.map(new DocumentTermDPHFormatter(broadcastTerms,broadcastNumDocuments,broadcastTermCountsInCorpus,broadcastAverageDocumentLength), Encoders.bean(DocumentTermDPHScores.class));
		//filters out documents in which none of the query vocabulary terms exist
		Dataset<DocumentTermDPHScores> filteredDocumentTermDPHDataset = documentTermDPHDataset.filter((FilterFunction<DocumentTermDPHScores>) d -> d.getDPHScores().size() > 0);
		//Maps previous dataset to a dataset comprising of every document and a hashmap of every query with their DPH score for that document
		Dataset<DocumentQueryDPHScores>  documentQueryDPHDataset = filteredDocumentTermDPHDataset.map(new DocumentQueryDPHFormatter(queryList),Encoders.bean(DocumentQueryDPHScores.class));

		
		//Obtains a dataset of DocumentRanking where every entry is a query and list of only 1 document
		Dataset<DocumentRanking> documentRankingDataset = documentQueryDPHDataset.flatMap(new DocumentRankingFormatter(), Encoders.bean(DocumentRanking.class));
		//groups the previous dataset by query
		KeyValueGroupedDataset<Query, DocumentRanking> keyValueGroupedQueryDocRank = documentRankingDataset.groupByKey((MapFunction<DocumentRanking, Query>) d -> {
					return d.getQuery();
				}, Encoders.bean(Query.class));
		//for each query, redundant documents are filtered out using TextDistance and a max total of 10 documents are returned at end of each reduce stage
		Dataset<Tuple2<Query, DocumentRanking>> documentRankingGroupedDataset = keyValueGroupedQueryDocRank.reduceGroups(new DocumentRedundancyFiltering());


		//Collecting the list of DocumentRanking objects
		List<DocumentRanking> finalResults = new ArrayList<DocumentRanking>();
		for (Tuple2<Query, DocumentRanking> tuple : documentRankingGroupedDataset.collectAsList()) {
			finalResults.add(tuple._2);
		}		

	
		return finalResults; // replace this with the the list of DocumentRanking output by your topology
	}
	
	
}
