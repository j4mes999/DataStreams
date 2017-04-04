/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package model;

/**
 *
 * @author root
 */
public class HashFunction {
    
    private int a;
    private int b;
    public static final int POWER = 22;
    private int maxR;

    
    public HashFunction(int a, int b){
        this.a = a;
        this.b = b;
        this.maxR = 0;
    }
    
    public int getMaxR() {
        return maxR;
    }

    public void setMaxR(int maxR) {
        this.maxR = maxR;
    }
    
    

    public int getA() {
        return a;
    }

    public void setA(int a) {
        this.a = a;
    }

    public int getB() {
        return b;
    }

    public void setB(int b) {
        this.b = b;
    }
    
    public long getHashValue(long element){
        return (long) ((this.a * element + this.b) % Math.pow(2, POWER));
    }
    
    
    
}