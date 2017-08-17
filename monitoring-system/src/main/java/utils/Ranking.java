package utils;

import java.io.Serializable;
import java.util.ArrayList;


public class Ranking implements Serializable {

    private ArrayList<RankItem> ranking;

    public ArrayList<RankItem> getRanking() {
        return ranking;
    }

    public void setRanking(ArrayList<RankItem> ranking) {
        this.ranking = ranking;
    }
}
