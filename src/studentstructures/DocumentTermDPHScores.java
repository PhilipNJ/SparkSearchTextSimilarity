package studentstructures;

import java.util.Map;

public class DocumentTermDPHScores {
	
	public Document document;
	public Map<String,Double> DPHScores;
	
	
	public DocumentTermDPHScores() {
		// TODO Auto-generated constructor stub
	}
	public DocumentTermDPHScores(Document document, Map<String, Double> dPHScores) {
		super();
		this.document = document;
		DPHScores = dPHScores;
	}
	public Document getDocument() {
		return document;
	}
	public void setDocument(Document document) {
		this.document = document;
	}
	public Map<String, Double> getDPHScores() {
		return DPHScores;
	}
	public void setDPHScores(Map<String, Double> dPHScores) {
		DPHScores = dPHScores;
	}
	
}
