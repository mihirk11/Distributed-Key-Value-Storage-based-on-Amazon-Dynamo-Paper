package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;

/**
 * Created by mihir on 4/11/16.
 */
class Node implements Serializable {
    Node(String id,String hash){
        this.id=id;
        this.hash=hash;
        port = String.valueOf((Integer.parseInt(id) * 2));
    }
    String id=null;
    String hash=null;
    String port=null;
}