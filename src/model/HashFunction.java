
package model;

/**
 * This class represents a hash function used
 * in the Flajolet-Martin Algorithm
 * @author Luis Gonzalez
 */
public class HashFunction {
    
    private int a;
    private int b;
    public static final int POWER = 5000000;
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
        //return (long) ((this.a * element + this.b) % POWER);
    }
    
    
    
}
