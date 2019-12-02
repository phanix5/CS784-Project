package com.myproj;

public class QueryContext {
    private String dt1;
    private String dt1c1;
    private String dt1c2;
    private String dt2;
    private String dt2c1;
    private String dt2c2;
    private Double tau;

    public QueryContext(String dt1, String dt1c1, String dt1c2, String dt2, String dt2c1, String dt2c2, Double tau) {
        this.dt1 = dt1;
        this.dt1c1 = dt1c1;
        this.dt1c2 = dt1c2;
        this.dt2 = dt2;
        this.dt2c1 = dt2c1;
        this.dt2c2 = dt2c2;
        this.tau = tau;
    }

    public String getDt1() {
        return dt1;
    }

    public void setDt1(String dt1) {
        this.dt1 = dt1;
    }

    public String getDt1c1() {
        return dt1c1;
    }

    public void setDt1c1(String dt1c1) {
        this.dt1c1 = dt1c1;
    }

    public String getDt1c2() {
        return dt1c2;
    }

    public void setDt1c2(String dt1c2) {
        this.dt1c2 = dt1c2;
    }

    public String getDt2() {
        return dt2;
    }

    public void setDt2(String dt2) {
        this.dt2 = dt2;
    }

    public String getDt2c1() {
        return dt2c1;
    }

    public void setDt2c1(String dt2c1) {
        this.dt2c1 = dt2c1;
    }

    public String getDt2c2() {
        return dt2c2;
    }

    public void setDt2c2(String dt2c2) {
        this.dt2c2 = dt2c2;
    }

    public Double getTau() {
        return tau;
    }

    public void setTau(Double tau) {
        this.tau = tau;
    }
    
    

    
}