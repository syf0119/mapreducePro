package com.it.fall;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements Writable {
     private  String oid;
     private String pid;
     private double money;

    public String getOid() {
        return oid;
    }

    public void setOid(String oid) {
        this.oid = oid;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public double getMoney() {
        return money;
    }

    public void setMoney(double money) {
        this.money = money;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(oid);
        out.writeUTF(pid);
        out.writeDouble(money);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.oid=in.readUTF();
        this.pid=in.readUTF();
        this.money=in.readDouble();
    }

    @Override
    public String toString() {
        return this.oid+"\t"+this.pid+"\t"+this.money+"\t";
    }
}
