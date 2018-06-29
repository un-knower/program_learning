package demo.hadoop.window;

import java.util.LinkedList;
import java.util.Queue;

/**
 * 模拟window 解决方案1
 */
public class SimpleMovingAverage implements SimpleWindow {
    private double sum = 0.0d;
    private int period;
    private final Queue<Double> window = new LinkedList<>();

    public SimpleMovingAverage(int period) {
        if (period < 1) {
            throw new IllegalArgumentException("period must be > 0");
        }
        this.period = period;
    }

    @Override
    public void addNeNumber(double number) {
        sum += number;
        window.add(number);
        if (window.size() > period) {
            sum -= window.remove();
        }
    }

    @Override
    public double getMovingAverage() {
        if (window.isEmpty()) {
            throw new IllegalArgumentException("average si undefined");
        }
        return sum / window.size();
    }
}
