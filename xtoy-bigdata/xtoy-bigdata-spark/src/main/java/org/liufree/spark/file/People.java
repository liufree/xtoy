package org.liufree.spark.file;

import java.io.Serializable;

/**
 * @author lwx
 * 9/19/19
 * liufreeo@gmail.com
 */
public class People implements Serializable {

    String name;

    String age;


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAge() {
        return age;
    }

    public void setAge(String age) {
        this.age = age;
    }
}
