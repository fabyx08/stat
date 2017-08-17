package utils;

public class Window {
    private int[] timeframes;  //osservazioni su base oraria
    private double[] consumeframes; //consumi
    private int currentIndex;
    private int size; // dimensione della finestra temporale
    private int estimatedTotal;
    private double estimatedSum;
    private String sensor;
    //private String city;

    public Window(int size) {

        this.timeframes = new int[size];
        this.consumeframes = new double[size];
        this.size = size;
        this.currentIndex = 0;
        this.estimatedTotal = 0;
        this.estimatedSum = 0;
        this.sensor = "";
        //this.city = "";

        for (int i = 0; i < size; i++) {
            timeframes[i] = 0;
            consumeframes[i] = 0;
        }

    }

    public int moveForward() {

        //scorro la finestra temporale di una posizione, tolgo dalla somma delle osservazioni e dal numero di osservazioni
        //quelle salvate in uno slot della finestra temporale


		/* Free the timeframe that is going to go out of the window */
        int lastTimeframeIndex = (currentIndex + 1) % size;

        int value = timeframes[lastTimeframeIndex];
        double valueSum = consumeframes[lastTimeframeIndex];

        timeframes[lastTimeframeIndex] = 0;
        consumeframes[lastTimeframeIndex] = 0;

        estimatedTotal -= value;
        estimatedSum -= valueSum;

		/* Move forward the current index */
        currentIndex = (currentIndex + 1) % size;

        return value;

    }

    public int moveForward(int positions) {

        int cumulativeValue = 0;

        for (int i = 0; i < positions; i++) {

            cumulativeValue += moveForward();

        }

        return cumulativeValue;
    }

    public void increment(double valueSum) {

        increment(1, valueSum);

    }


    public void increment(int value, double valueSum) {

        //aggiungo value osservazioni ed aggiungo valueSum (consumo)

        timeframes[currentIndex] = timeframes[currentIndex] + value;
        consumeframes[currentIndex] = consumeframes[currentIndex] + valueSum;

        estimatedTotal += value;
        estimatedSum += valueSum;

    }

    public void decrement(int value, double valueSum) {
        timeframes[currentIndex] = timeframes[currentIndex] - value;
        consumeframes[currentIndex] = consumeframes[currentIndex] - valueSum;

        estimatedTotal -= value;
        estimatedSum -= valueSum;
    }

    @Override
    public String toString() {

        String s = "[";

        for (int i = 0; i < timeframes.length; i++) {

            s += timeframes[i];

            if (i < (timeframes.length - 1))
                s += ", ";

        }

        s += "]";
        return s;

    }

    public int getEstimatedTotal() {
        return estimatedTotal;
    }

    public double getEstimatedSum() {
        return estimatedSum;
    }

    public int computeTotal() {

        //calcolo numero di osservazioni

        int total = 0;
        for (int i = 0; i < timeframes.length; i++)
            total += timeframes[i];
        return total;

    }


    public double computeSum() {

        //somma di tutti i consumi

        double total = 0;
        for (int i = 0; i < consumeframes.length; size++) {
            total += consumeframes[i];
        }
        return total;

    }

    public String getSensor() {
        return sensor;
    }

    public void setSensor(String sensor) {
        this.sensor = sensor;
    }

    /*public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }*/
}
