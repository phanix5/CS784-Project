package com.myproj;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class TwoPath implements Query {

    private Connection dbConn;
    Map<String,Integer[]> freeVarMap = new TreeMap<>();
    private TreeNode rootNode;
    private List<String[]> tableR1,tableR2;
    private List<String> freeVarVals;
    private Double tau;
    private QueryContext ctx;

    public TwoPath(Connection conn, QueryContext ctx) {
        dbConn = conn;
        this.tau=ctx.getTau();
        this.ctx=ctx;
        freeVarVals = new ArrayList<>();
        tableR1 = new ArrayList<>();
        tableR2 = new ArrayList<>();
    }

    class TreeNode{
        private Integer startIndex, endIndex;
        private List<Integer[]> interval;
        private TreeNode left,right;
        private Map<String, Integer> dictionary;

        TreeNode(List<Integer[]> invl, int s, int e){
            left=null;
            right=null;
            interval=invl;
            startIndex=s;
            endIndex=e;
            dictionary = new HashMap<String, Integer>();
        }

        public int getR1Count(int index){
            if(index>=startIndex && index<=endIndex){
                return interval.get(index)[0];
            }
            return 0;
        }

        public int getR2Count(int index){
            if(index>=startIndex && index<=endIndex){
                return interval.get(index)[1];
            }
            return 0;
        }
        
        public Double getTotalTimeBound(){
            int leftCount=0,rightCount=0;
            for(int i=startIndex;i<=endIndex;i++){
                leftCount+=interval.get(i)[0];
                rightCount+=interval.get(i)[1];
            }
            return Math.sqrt(leftCount*rightCount);
        }

        public TreeNode getLeft() {
            return left;
        }

        public void setLeft(TreeNode left) {
            this.left = left;
        }

        public TreeNode getRight() {
            return right;
        }

        public void setRight(TreeNode right) {
            this.right = right;
        }
    }

    @Override
    public void preProcess() {
        Statement st;
        Map<String,Integer> freeVarR1Map = new HashMap<>();
        Map<String,Integer> freeVarR2Map = new HashMap<>();
        Set<String> freeVarValSet = new HashSet<>();

        try {
            st = dbConn.createStatement();
            ResultSet rs = st.executeQuery("SELECT COUNT(Y),Y FROM R1 GROUP BY Y ORDER BY Y");
            while(rs.next()){
                freeVarR1Map.put(rs.getString("y"), rs.getInt("count"));
                freeVarValSet.add(rs.getString("y"));
            }

            rs = st.executeQuery("SELECT COUNT(X),X FROM R2 GROUP BY X ORDER BY X");
            while(rs.next()){
                freeVarR2Map.put(rs.getString("x"), rs.getInt("count"));
                freeVarValSet.add(rs.getString("x"));
            }

            for(String val : freeVarValSet){
                freeVarMap.put(val,new Integer[]{freeVarR1Map.getOrDefault(val,0),freeVarR2Map.getOrDefault(val,0)});
            }
            
            List<Integer[]> tupleCount = new ArrayList<>();
            freeVarMap.forEach((k,v)-> tupleCount.add(v));
            freeVarMap.forEach((k,v)-> freeVarVals.add(k));
            buildDelayBalancedTree(tupleCount, tau);
            buildDictionary();

        } catch (SQLException e) {
            e.printStackTrace();
        }
        

    }

    private void buildTree(TreeNode node, int level){
        
        Double fullIntervalTimeBound = node.getTotalTimeBound();
        //System.out.println(level+" : "+node.startIndex+" , "+node.endIndex);
        if(node.startIndex==node.endIndex || fullIntervalTimeBound<=tau/Math.pow(2, level/2.0))return;
        int r1Count=0,r2Count=0,index=node.startIndex;
        double currTimeBound=0;
        while(index<=node.endIndex){
            r1Count+=node.getR1Count(index);
            r2Count+=node.getR2Count(index);
            currTimeBound=Math.sqrt(r1Count*r2Count);
            if(currTimeBound > fullIntervalTimeBound/2)break;
            index++;
        }
        // A single free variable's valuation's degree is higher than tau, can't do much in this case
        if(index==node.startIndex || index>node.endIndex)return;
        
        node.setLeft(new TreeNode(node.interval, node.startIndex,index-1));
        node.setRight(new TreeNode(node.interval,index,node.endIndex));
        buildTree(node.left, level+1);
        buildTree(node.right, level+1);
    }

    private void buildDelayBalancedTree(List<Integer[]> interval, Double tau){
        rootNode=new TreeNode(interval,0,interval.size()-1);
        buildTree(rootNode, 0);
    }

    private void buildDictionary(){
        // Just pull both the tables into memory, instead of db queries for
        // every node.
        Statement st;
        try {
            st = dbConn.createStatement();
            ResultSet rs = st.executeQuery("SELECT * FROM R1 ORDER BY Y");
            while(rs.next()){
                tableR1.add(new String[]{rs.getString("Y"),rs.getString("X")});
            }
            rs = st.executeQuery("SELECT * FROM R2 ORDER BY X");
            while(rs.next()){
                tableR2.add(new String[]{rs.getString("X"),rs.getString("Y")});
            }
            buildDictionaryUtil(rootNode, 0);
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    private void buildDictionaryUtil(TreeNode node, int level){
        List<String[]> r1Sub = semiJoinWithNodeInterval(tableR1, node);
        List<String[]> r2Sub = semiJoinWithNodeInterval(tableR2, node);

        // Hash lookup for count of bound variables in each table
        Map<String,Integer> r1VbCount = new HashMap<>();
        Map<String,Integer> r2VbCount = new HashMap<>();

        r1Sub.forEach((fact)->r1VbCount.merge(fact[1], 1, (a,b)->a+b));
        r2Sub.forEach((fact)->r2VbCount.merge(fact[1], 1, (a,b)->a+b));

        /*
            Do a join on the free variable, or should the db do it?
            Will need a online-join though, otherwise the whole result will be materialized
            by the DB
        */

        int i=0,j=0;
        while(i<r1Sub.size() && j<r2Sub.size()){
            String r1FreeVal = r1Sub.get(i)[0], r2FreeVal = r2Sub.get(j)[0];
            if(r1FreeVal.equals(r2FreeVal)){
                // free variable value is same, find all vb's
                int ii=i;
                while(ii<r1Sub.size() && r1FreeVal.equals(r1Sub.get(ii)[0]))ii++;
                int jj=j;
                while(jj<r2Sub.size() && r2FreeVal.equals(r2Sub.get(jj)[0]))jj++;
                for(int i1=i;i1<ii;i1++){
                    for(int j1=j;j1<jj;j1++){
                        String vb1 = r1Sub.get(i1)[1],vb2=r2Sub.get(j1)[1];
                        String key = generateKey(vb1, vb2);
                        if(!node.dictionary.containsKey(key)){
                            // check if this vb pair is heavy
                            if(Math.sqrt(r1VbCount.get(vb1)*r2VbCount.get(vb2))>tau/Math.pow(2, level/2.0)){
                                node.dictionary.put(key, 1);
                            }else{
                                node.dictionary.put(key, 0);
                            }
                        }
                    }
                }
                i=ii;
                j=jj;
            }else if(r1FreeVal.compareTo(r2FreeVal)>0)j++;
            else i++;
        }
        if(node.left!=null)buildDictionaryUtil(node.left, level+1);
        if(node.right!=null)buildDictionaryUtil(node.right, level+1);

    }

    private List<String[]> semiJoinWithNodeInterval(List<String[]> baseRelation, TreeNode node){
        String leftBound = freeVarVals.get(node.startIndex);
        String rightBound = freeVarVals.get(node.endIndex);
        List<String[]> resultTable = new ArrayList<>();
        baseRelation.forEach((v)->{
            String val=v[0];
            if(val.compareTo(leftBound)>=0 && val.compareTo(rightBound)<=0){
                resultTable.add(v);
            }
        });
        return resultTable;
    }

    @Override
    public void evaluate(String[] boundVals) {
        String vb1=boundVals[0], vb2=boundVals[1];

        // DB will evaluate the join, once per tree node
        evaluateUtil(rootNode, vb1, vb2);

    }

    private void evaluateUtil(TreeNode node, String vb1, String vb2){
        String key = generateKey(vb1, vb2);
        Statement st;
        if(node.dictionary.containsKey(key)){
            if(node.dictionary.get(key)==1 && (node.left!=null || node.right!=null)){
                // Heavy valuation, move to child nodes if present
                if(node.left!=null)evaluateUtil(node.left, vb1, vb2);
                if(node.right!=null)evaluateUtil(node.right, vb1, vb2);
            }else{
                try {
                    st=dbConn.createStatement();
                    ResultSet rs = st.executeQuery("select r1.y from r1 inner join r2 on r1.y=r2.x and r1.y between "
                            + freeVarVals.get(node.startIndex) + " and " + freeVarVals.get(node.endIndex) + " and r1.x="
                            + vb1 + " and r2.y=" + vb2);
                    while(rs.next()){
                        System.out.println(rs.getString("y"));
                    }
                } catch (SQLException e) {
                    
                    e.printStackTrace();
                }
            }
        }
        // if dictionary doesn't contain key, then this interval does not have any valuation
    }

    private String generateKey(String a, String b){
        return a+"##"+b;
    }
    

}