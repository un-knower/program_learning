package demo.spark.utils;

public class Outer {
    public static class Innter {
        public Innter(){}
        public Innter(String name) {
            System.out.println(name);
        }
        public Innter getInnter(String name) {
            return new Innter(name);
        }
    }
}
