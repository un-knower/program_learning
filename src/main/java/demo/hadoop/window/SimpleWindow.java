package demo.hadoop.window;

public interface SimpleWindow {
    /**
     * 添加一个新数据
     * @param number
     */
    public void addNeNumber(double number);

    /**
     * 得到平均值
     * @return
     */
    public double getMovingAverage();
}
