

package model;

import java.util.ArrayList;
import java.util.List;

/**
 * This class represents a Group that contains many hash functions
 * @author Luis Gonzalez
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
