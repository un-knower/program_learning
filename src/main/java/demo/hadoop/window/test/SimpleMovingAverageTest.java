package demo.hadoop.window.test;

public class SimpleMovingAverageTest {
    public static void main(String args[]) {
        double data[] = {10,18,20,30,24,33,27};
        int[] allWindowSizes = {3, 4};
        for (int windowSize : allWindowSizes) {
//            SimpleMovingAverage simpleMovingAverage = new SimpleMovingAverage(windowSize);
            SimpleMovingAverageUsingArray simpleMovingAverage = new SimpleMovingAverageUsingArray(windowSize);
            for (double x : data) {
                simpleMovingAverage.addNeNumber(x);
                System.out.println("number= "+ x +", average= "+simpleMovingAverage.getMovingAverage());
            }
        }
    }
}

/*
        number= 10.0, average= 10.0
        number= 18.0, average= 14.0
        number= 20.0, average= 16.0
        number= 30.0, average= 22.666666666666668
        number= 24.0, average= 24.666666666666668
        number= 33.0, average= 29.0
        number= 27.0, average= 28.0
        number= 10.0, average= 10.0
        number= 18.0, average= 14.0
        number= 20.0, average= 16.0
        number= 30.0, average= 19.5
        number= 24.0, average= 23.0
        number= 33.0, average= 26.75
        number= 27.0, average= 28.5
*/