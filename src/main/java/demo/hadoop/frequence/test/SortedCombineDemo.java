package demo.hadoop.frequence.test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class SortedCombineDemo {

    /**
     * 递归查找有序组合
     * @param elements
     * @param n
     * @param <T>
     * @return
     */
    public static <T extends Comparable<? super T>> List<List<T>>
            findSortedCombinations(Collection<T> elements, int n) {

        List<List<T>> result = new ArrayList<>();
        // 处理递归的初始步骤
        if (n == 0) {
            result.add(new ArrayList<T>());
            return result;
        }

        // 处理n-1递归
        List<List<T>> combinations = findSortedCombinations(elements, n - 1);
        for (List<T> combination : combinations) {
            for (T element : elements) {
                if (combination.contains(element)) {
                    continue;
                }
                List<T> list = new ArrayList<>();
                list.addAll(combination);
                list.add(element);

                //对元素排序，以避免重复的元素
                Collections.sort(list);
                if (result.contains(list)) {
                    continue;
                }
                result.add(list);
            }
        }

        return result;

    }
    public static void main(String args[]) {
        List<String> list = new ArrayList<>();
        list.add("a");
        list.add("b");
        list.add("c");
        list.add("d");

        List<List<String>> sortedCombinations = findSortedCombinations(list, 2);
        printList(sortedCombinations);
        List<List<String>> sortedCombinations2 = findSortedCombinations(list, 3);
        printList(sortedCombinations2);


    }



    public static void printList(List<List<String>> arrays) {
        StringBuffer stringBuffer = new StringBuffer();
        for (List<String> li : arrays) {
            stringBuffer.setLength(0);
            for (String e : li) {
                stringBuffer.append(e);
            }
            System.out.println(stringBuffer.toString());
        }
    }
}
