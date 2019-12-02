package com.myproj;

public interface Query {
    void preProcess();
    void evaluate(String[] bvals);
}