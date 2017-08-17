package utils;

public class Sort {

    private double[] array;

    public Sort(double[] sort) {
        this.array = sort;
    }

    public void bubbleSort() {

        for (int i = 0; i < this.array.length; i++) {
            boolean flag = false;
            for (int j = 0; j < this.array.length - 1; j++) {
                if (this.array[j] > this.array[j + 1]) {
                    double k = this.array[j];
                    this.array[j] = this.array[j + 1];
                    this.array[j + 1] = k;
                    flag = true;
                }


            }
            if (!flag) break;
        }
    }

    public double[] getArray() {
        return array;
    }

    public void setArray(double[] array) {
        this.array = array;
    }
}
