package utils;


public class PQuantile {

    private double[] q;            //consume array
    private int[] n;              //position array
    private double[] nd;           //desired marker position
    private double[] dn;           //increment array
    private int d;

    public PQuantile(double p, double x1, double x2, double x3, double x4, double x5) {

        // sort the first five observations
        this.q = new double[5];
        this.q[0] = x1;
        this.q[1] = x2;
        this.q[2] = x3;
        this.q[3] = x4;
        this.q[4] = x5;
        Sort sort = new Sort(q);
        sort.bubbleSort();
        this.q = sort.getArray();
        this.n = new int[5];
        for (int i = 0; i < this.n.length; i++) {
            n[i] = i + 1;
        }

        // desired marker position initialization
        this.nd = new double[5];
        this.nd[0] = 1;
        this.nd[1] = 1 + 2 * p;
        this.nd[2] = 1 + 4 * p;
        this.nd[3] = 3 + 2 * p;
        this.nd[4] = 5;

        //increment array initialization
        this.dn = new double[5];
        this.dn[0] = 0;
        this.dn[1] = p / 2;
        this.dn[2] = p;
        this.dn[3] = (1 + p) / 2;
        this.dn[4] = 1;

    }

    //from the 6-th observation
    public void observation(double x) {

        int k = findCell(x);  //find cell and adjust extreme values q[i] if necessary
        increment(k);       // increment positions of marker k+1 through five
        adjustHeigths();    //adjust heigths of markers 2-4 if necessary

    }


    private int findCell(double x) {
        if (x < this.q[0]) {
            q[0] = x;
            return 0;
        } else if (x >= this.q[0] && x < this.q[1]) {
            return 0;
        } else if (x >= this.q[1] && x < this.q[2]) {
            return 1;
        } else if (x >= this.q[2] && x < this.q[3]) {
            return 2;
        } else if (x >= this.q[3] && x <= this.q[4]) {
            return 3;
        } else if (x > this.q[4]) {
            q[4] = x;
            return 3;
        }
        System.out.println("ERROR\n");
        return -1;

    }


    private void increment(int k) {
        for (int i = k; i < this.n.length; i++) {
            n[i] = n[i] + 1;
        }
        for (int j = 0; j < this.nd.length; j++) {
            nd[j] = nd[j] + dn[j];
        }
    }

    private void adjustHeigths() {
        double s;
        double pp;
        double lp;
        for (int i = 1; i < 3; i++) {
            this.d = (int) nd[i] - n[i];
            if ((d >= 1) && (n[i + 1] - n[i] > 1) || (d <= -1) && (n[i - 1] - n[i] < -1)) {
                d = (int) Math.signum((float) nd[i] - n[i]);
                pp = PPformula(i, Integer.parseInt(String.valueOf(d)));
                s = q[i];
                if (s > q[i - 1] && s < q[i + 1]) {
                    q[i] = pp;
                } else {
                    lp = linearPrediction(i, Integer.parseInt(String.valueOf(d)));
                    q[i] = lp;
                }
                n[i] = n[i] + Integer.parseInt(String.valueOf(d));
            }
        }
    }


    private double linearPrediction(int i, int d) {
        double lp;
        lp = q[i] + d * ((q[i + d] - q[i]) / (n[i + d] - n[i]));
        return lp;
    }

    private double PPformula(int i, int d) {
        double r = q[i] + d / (n[i + 1] - n[i - 1]) *
                (((n[i] - n[i - 1] + d) * ((q[i + 1] - q[i]) / (n[i + 1] - n[i]))) +
                        ((n[i + 1] - n[i] + d) * ((q[i] - q[i - 1]) / (n[i] - n[i - 1]))));
        return r;
    }


    public double estimatedPQuantile() {
        return q[2];
    }

}
