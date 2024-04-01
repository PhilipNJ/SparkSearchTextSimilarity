package studentfunctions;

import org.apache.spark.api.java.function.MapFunction;

import providedstructures.ContentItem;
import providedstructures.NewsArticle;
import providedutilities.TextPreProcessor;
import studentstructures.Document;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NewsArticleTokenizer implements MapFunction<NewsArticle, Document> {
    private static final long serialVersionUID = 6475166483071609778L;

    private transient TextPreProcessor processor;
   
	@Override
    public Document call(NewsArticle newsArticle) throws Exception {
        if (processor==null) processor = new TextPreProcessor();
        
        
        ArrayList<ContentItem> listOfContentItems = new ArrayList<>();

        for (ContentItem contentItem :  newsArticle.getContents()) {
            if (listOfContentItems.size() == 5)//if 5 paragraphs are obtained stop
                break;
            if (contentItem!=null && contentItem.getSubtype() != null && contentItem.getSubtype().equals("paragraph")) { //if content is a paragraph add it to list
                listOfContentItems.add(contentItem);
            }
        }
        //creating a string of news article title and first 5 paragraphs
        StringBuilder stringContent = new StringBuilder();
        String title="";
        if(newsArticle.getTitle() != null) {
            title = newsArticle.getTitle();
            stringContent.append(title);
        }

        for (ContentItem contentItem : listOfContentItems) {
            stringContent.append(contentItem.getContent());
        }

        List<String> contentTerms = processor.process(stringContent.toString());//getting list of terms after preprocessing string
        
        Map<String, Short> contentTermsCountsMap = new HashMap<>();

        for (String term : contentTerms)//storing terms and their corresponding frequencies in a hashmap
        {
        	if(contentTermsCountsMap.containsKey(term))
        		contentTermsCountsMap.put(term, (short)(contentTermsCountsMap.get(term)+1));
        	else
        		contentTermsCountsMap.put(term, (short)1);
        }

        String docId = newsArticle.getId();

        Document document = new Document(docId, title, newsArticle, stringContent.toString(), contentTermsCountsMap);

        return document;
    }
}
