package utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;


public class TopKRanking {
    private static final int NOT_PRESENT = -1;
    private Comparator<RankItem> comparator = null;
    private ArrayList<RankItem> ranking = null;
    private int topK;

    public TopKRanking(int k) {
        this.comparator = new RankItemComparator();
        this.ranking = new ArrayList<RankItem>();
        this.topK = k;
    }

    public boolean update(RankItem item) { //true if update needed or false

        int sizePreUpdate = ranking.size();
        int oldPosition = indexOf(item); //TUTTO BENE

        if (oldPosition != NOT_PRESENT) {
            ranking.remove(item);
        }

        int newPosition = add(item); // new item position

        int sizePostUpdate = ranking.size(); // new ranking size

        if (newPosition == oldPosition &&
                sizePreUpdate == sizePostUpdate) {

			/* do not notify position changed */
            return false;

        } else if (newPosition > topK - 1) {

			/* do not notify position changed in the lower side of the ranking */
            return false;
        }

        return true;

    }

    public int add(RankItem item) {
        int pos = Collections.binarySearch(ranking, item, comparator);
        int insertionPoint = pos > -1 ? pos : -pos - 1;
        if (insertionPoint < 0)
            ranking.add(-insertionPoint - 1, item);
        else
            ranking.add(insertionPoint, item);
        return insertionPoint;
    }

    public void remove(RankItem item) {
        ranking.remove(item);
    }

    public int indexOf(RankItem item) {
        for (int i = 0; i < ranking.size(); i++) {
            if (item.equals(ranking.get(i)))
                return i;
        }
        return NOT_PRESENT;
    }

    public boolean containsElement(RankItem item) {
        return ranking.contains(item);
    }

    public Ranking getTopK() {

        ArrayList<RankItem> top = new ArrayList<RankItem>();

        if (ranking.isEmpty()) {
            Ranking topKRanking = new Ranking();
            topKRanking.setRanking(top);
            return topKRanking;
        }

        int elems = Math.min(topK, ranking.size());

        for (int i = 0; i < elems; i++) {
            top.add(ranking.get(i));
        }

        Ranking topKRanking = new Ranking();
        topKRanking.setRanking(top);
        return topKRanking;

    }

    @Override
    public String toString() {
        return ranking.toString();
    }
}
