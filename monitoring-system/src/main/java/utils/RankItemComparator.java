package utils;

import java.util.Comparator;


public class RankItemComparator implements Comparator<RankItem> {

    @Override
    public int compare(RankItem r1, RankItem r2) {

        Long expectedTTL1 = r1.getInstallationTimestamp() + r1.getMeanExpirationTime();
        Long expectedTTL2 = r2.getInstallationTimestamp() + r2.getMeanExpirationTime();

        return expectedTTL1.compareTo(expectedTTL2);
    }
}
