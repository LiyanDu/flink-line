package com.flink.models;


/*
* @author Liyan
* @create 2022/07/17 21:26
* */

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Setter
@Getter
public class Student implements Serializable {

    private static final long serialVersionUID = -3247106837870523911L;

    private int id;

    private String name;

    private int age;

    private String createDate;
}
