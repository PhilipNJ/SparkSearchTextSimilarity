package studentstructures;

import providedstructures.Query;

import java.util.Map;

public class DocumentQueryDPHScores {
    public Document document;
    public Map<Query,Double> DPHScores;

    public DocumentQueryDPHScores() {
    }

    public DocumentQueryDPHScores(Document document, Map<Query, Double> DPHScores) {
        super();
        this.document = document;
        this.DPHScores = DPHScores;
    }

    public Map<Query, Double> getDPHScores() {
        return DPHScores;
    }

    public void setDPHScores(Map<Query, Double> DPHScores) {
        this.DPHScores = DPHScores;
    }

    public Document getDocument() {
        return document;
    }

    public void setDocument(Document document) {
        this.document = document;
    }
}
