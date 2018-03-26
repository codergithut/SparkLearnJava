package com.aggregate;

import java.io.Serializable;

public class AvgCount implements Serializable {

    /**
     * 总数
     */
    public int total;

    /**
     * 个数
     */
    public int num;

    public AvgCount(int total, int num) {
        this.total = total;
        this.num = num;
    }

    public double avg() {
        return total/(double) num;
    }

    public int getTotal() {
        return total;
    }

    public void setTotal(int total) {
        this.total = total;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }
}
