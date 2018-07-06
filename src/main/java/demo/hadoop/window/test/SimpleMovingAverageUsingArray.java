package demo.hadoop.window.test;

import demo.hadoop.window.SimpleWindow;

/**
 * 模拟window 解决方案2
 * 使用数组
 */
public class SimpleMovingAverageUsingArray implements SimpleWindow {
    private double sum = 0.0d;
    private int period;
    private double[] window = null;
    private int pointer = 0;
    private int size = 0;

    public SimpleMovingAverageUsingArray(int period) {
        if (period < 1) {
            throw new IllegalArgumentException("period must be > 0");
        }
        this.period = period;
        window = new double[period];
    }

    @Override
    public void addNeNumber(double number) {
        sum += number;
        if (size < period) {
            window[pointer++] = number;
            size ++;
        } else {
            pointer = pointer % period;
            sum -= window[pointer];
            window[pointer++] = number;
        }
    }

    @Override
    public double getMovingAverage() {
        if (size == 0) {
            throw  new IllegalArgumentException("average is undefined");
        }
        return sum / size;
    }
}
