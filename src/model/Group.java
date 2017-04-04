/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package model;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author root
 */
public class Group {
    
    private List<HashFunction> functions = new ArrayList<>();
    
    

    public List<HashFunction> getFunctions() {
        return functions;
    }

    public void setFunctions(List<HashFunction> functions) {
        this.functions = functions;
    }
    
    public int getSize(){
        return this.functions.size();
    }
    
    public void addFunction(HashFunction hf){
        this.functions.add(hf);
    }
    
}
