package demo.java.collection;

import java.util.*;

/**
 * 使用TreeMap<Integer, List<Student>> 可以是key相同的value都保存下来
 *
 */
public class TreeMapTest2 {
    public static void main(String args[]) {
        TreeMap<Integer, List<Student>> studentTreeMap2 = new TreeMap<>(new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return o1 - o2;
            }
        });

        List<Student> studentList = new ArrayList<>();
        studentList.add(new Student("a", 23));
        studentList.add(new Student("b", 13));
        studentList.add(new Student("c", 30));
        studentList.add(new Student("d", 11));
        studentList.add(new Student("e", 13));
        studentList.add(new Student("f", 5));
        studentList.add(new Student("g", 10));

        for (Student student : studentList) {
            if (studentTreeMap2.containsKey(student.age)) {
                studentTreeMap2.get(student.age).add(student);
            } else {
                List<Student> arr = new ArrayList<>();
                arr.add(student);
                studentTreeMap2.put(student.age, arr);
            }
        }


        Set<Integer> keySet = studentTreeMap2.keySet();
        Iterator<Integer> keyIter = keySet.iterator();
        while (keyIter.hasNext()) {
            Integer age = keyIter.next();
            List<Student> students = studentTreeMap2.get(age);
            for (Student s :students) {
                System.out.print(s+"\t");
            }

            System.out.println();

        }

    }
}


/* 输出结果
        Student{name='f', age=5}
        Student{name='g', age=10}
        Student{name='d', age=11}
        Student{name='b', age=13}	Student{name='e', age=13}
        Student{name='a', age=23}
        Student{name='c', age=30}
*/