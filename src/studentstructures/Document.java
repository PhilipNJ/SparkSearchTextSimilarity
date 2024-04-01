package studentstructures;

import java.io.Serializable;
import java.util.Map;

import providedstructures.NewsArticle;

public class Document implements Serializable {

    private static final long serialVersionUID = 7860293794078412249L;

    String docId;
    String title; // Title is used for filtering out text distance
    NewsArticle article;
	String originalContent; // The original content unaltered
    Map<String, Short> contentTermCountMap;

    public Document() {}

    public Document(String docId, String title, NewsArticle article,String originalContent, Map<String, Short> contentTermCountMap) {
        this.docId = docId;
        this.title = title;
        this.article = article;
        this.originalContent = originalContent;
        this.contentTermCountMap = contentTermCountMap;
    }

    public String getDocId() {
        return docId;
    }

    public void setDocId(String docId) {
        this.docId = docId;
    }

    public String getTitle() {
        return title;
    }

    public String getOriginalContent() {
        return originalContent;
    }
    public NewsArticle getArticle() {
		return article;
	}

	public void setArticle(NewsArticle article) {
		this.article = article;
	}


    public Map<String, Short> getContentTermCountMap() {
        return contentTermCountMap;
    }

    public void setContentTermCountMap(Map<String, Short> contentTermCountMap) {
        this.contentTermCountMap = contentTermCountMap;
    }

    public void setTitle(String title) {
		this.title = title;
	}

	public void setOriginalContent(String originalContent) {
		this.originalContent = originalContent;
	}
}
