package demo.java.collection;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeMap;

class Student {
    String name;
    int age;

    public Student(String name, int age) {
         this.name = name;
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "Student{" +
                "name='" + name + '\'' +
                ", age=" + age +
                '}';
    }
}
public class TreeMapTest {
    public static void main(String args[]) {
        TreeMap<Student, String> studentTreeMap = new TreeMap<>(new Comparator<Student>() {
            @Override
            public int compare(Student o1, Student o2) {
                return o1.age - o2.age;
            }
        });
        studentTreeMap.put(new Student("a", 23), "a");
        studentTreeMap.put(new Student("b", 13), "b");
        studentTreeMap.put(new Student("c", 30), "c");
        studentTreeMap.put(new Student("d", 11), "d");
        studentTreeMap.put(new Student("e", 13), "e");  // 这个值不会出现
        studentTreeMap.put(new Student("f", 5), "f");
        studentTreeMap.put(new Student("g", 10), "g");

        Set<Student> studentsKey = studentTreeMap.keySet();
        Iterator<Student> studentsKeyIter = studentsKey.iterator();

        while (studentsKeyIter.hasNext()) {
            Student next = studentsKeyIter.next();
            System.out.println(next);


        }

    }
}
