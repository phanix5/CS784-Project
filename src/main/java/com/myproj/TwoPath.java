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
    private int maxTreeDepth;

    public TwoPath(Connection conn, QueryContext ctx) {
        dbConn = conn;
        this.tau=ctx.getTau();
        this.ctx=ctx;
        maxTreeDepth=0;
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
            Double leftCount=0.0,rightCount=0.0;
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
            ResultSet rs = st.executeQuery(getFreeVariableQuery(ctx.getDt1(), ctx.getDt1c2()));
            int maxCount=0;
            while(rs.next()){
                freeVarR1Map.put(rs.getString(ctx.getDt1c2()), rs.getInt("count"));
                freeVarValSet.add(rs.getString(ctx.getDt1c2()));
            }
            rs = st.executeQuery(getFreeVariableQuery(ctx.getDt2(), ctx.getDt2c1()));
            while(rs.next()){
                freeVarR2Map.put(rs.getString(ctx.getDt2c1()), rs.getInt("count"));
                freeVarValSet.add(rs.getString(ctx.getDt2c1()));
                maxCount = Math.max(maxCount, rs.getInt("count"));
            }
            
            for(String val : freeVarValSet){
                freeVarMap.put(val,new Integer[]{freeVarR1Map.getOrDefault(val,0),freeVarR2Map.getOrDefault(val,0)});
            }
            
            List<Integer[]> tupleCount = new ArrayList<>();
            freeVarMap.forEach((k,v)-> tupleCount.add(v));
            freeVarMap.forEach((k,v)-> freeVarVals.add(k));
            buildDelayBalancedTree(tupleCount, tau);
            if(maxTreeDepth > 10){
                System.out.println("Warning: Max tree depth too large, select a higher value of tau");
                System.exit(1);
            }
            buildDictionary();
            System.out.println("Memory Usage: "+getMemoryStats(rootNode));

        } catch (SQLException e) {
            e.printStackTrace();
        }
        

    }

    /*
      No sanitization at all, woohoo!
    */
    private String getFreeVariableQuery(String dtName, String colName){
        return "SELECT COUNT("+colName+"),"+colName+" FROM "+dtName+" GROUP BY "+colName+" ORDER BY " + colName;
    }

    /*
        Come on, drop table -- :p
    */
    private String getTableQuery(String dtName, String col1, String col2, String order){
        return "SELECT "+col1+","+col2+" FROM "+dtName+" ORDER BY "+order;
    }

    private String getJoinQuery(String rangeLeft, String rangeRight, String vb1, String vb2){
        String r1x = ctx.getDt1()+"."+ctx.getDt1c1();
        String r1y = ctx.getDt1()+"."+ctx.getDt1c2();
        String r2x = ctx.getDt2()+"."+ctx.getDt2c1();
        String r2y = ctx.getDt2()+"."+ctx.getDt2c2();

        String query = "select "+r1y+" from "+ctx.getDt1()+" inner join "+ctx.getDt2()+" on "+r1y+"="
        +r2x+" and "+r1y+" between "+ rangeLeft + " and " + rangeRight + " and "+r1x+"=" + vb1 + " and "+r2y+"=" + vb2;
        return query;
    }

    private void buildTree(TreeNode node, int level){
        maxTreeDepth=Math.max(maxTreeDepth,level);
        Double fullIntervalTimeBound = node.getTotalTimeBound();
        if(node.startIndex==node.endIndex || fullIntervalTimeBound<=tau/Math.pow(2, level/2.0))return;
        double r1Count=.0,r2Count=.0;
        int index=node.startIndex;
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
            ResultSet rs = st.executeQuery(getTableQuery(ctx.getDt1(), ctx.getDt1c1(), ctx.getDt1c2(), ctx.getDt1c2()));
            while(rs.next()){
                tableR1.add(new String[]{rs.getString(ctx.getDt1c2()),rs.getString(ctx.getDt1c1())});
            }
            rs = st.executeQuery(getTableQuery(ctx.getDt2(), ctx.getDt2c1(), ctx.getDt2c2(), ctx.getDt2c1()));
            while(rs.next()){
                tableR2.add(new String[]{rs.getString(ctx.getDt2c1()),rs.getString(ctx.getDt2c2())});
            }
            buildDictionaryUtil(rootNode, 0);
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    private void buildDictionaryUtil(TreeNode node, int level){
        //System.out.println(level);
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
                            /**
                             * Ignore the vb pair if not tau-heavy. In the paper the dictionary somehow knows
                             * if a vb pair has a result in a node's interval. This means information about
                             * light pairs are also stored (??). This would mean in the worst case, every (x,y)
                             * pair is actually stored in the dict which means O(N^2) space, completely negating
                             * the whole algorithm. Or this is what is my understanding.
                             * 
                             * So here's what I will do. I will not store light vb in dict and during evalaution,
                             * if a pair does not appear in a dict then the join will be evaluated, and the result maybe
                             * empty. Only when the vb pair is in th dict, I'll move to the child nodes to continue
                             * evaluation.
                             */
                                //node.dictionary.put(key, 0);
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
        long stTime = System.currentTimeMillis();
        // DB will evaluate the join, once per tree node
        long rowCount = evaluateUtil(rootNode, vb1, vb2);
        long edTime = System.currentTimeMillis();
        System.out.println("Result size: "+rowCount);
        System.out.println("Answering Time (in ms): "+(edTime-stTime));
    }

    private long evaluateUtil(TreeNode node, String vb1, String vb2){
        String key = generateKey(vb1, vb2);
        Statement st;
        long rowCount=0;
        if(node.dictionary.containsKey(key) && node.dictionary.get(key)==1 && (node.left!=null || node.right!=null)){
            // Heavy valuation, move to child nodes if present
            if(node.left!=null)rowCount+=evaluateUtil(node.left, vb1, vb2);
            if(node.right!=null)rowCount+=evaluateUtil(node.right, vb1, vb2);
        }else{
            try {
                st=dbConn.createStatement();
                
                //System.out.println(getJoinQuery(freeVarVals.get(node.startIndex), freeVarVals.get(node.endIndex), vb1, vb2));
                ResultSet rs = st.executeQuery(getJoinQuery(freeVarVals.get(node.startIndex), freeVarVals.get(node.endIndex), vb1, vb2));
                while(rs.next()){
                    //System.out.println(rs.getString(ctx.getDt1c2()));
                    rowCount++;
                }
                
                
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return rowCount;
    }

    private int getMemoryStats(TreeNode node){
        int memUsage = node.dictionary.size()*2 + 16; // treenode is 16 bytes min
        if(node.left!=null){
            memUsage+=getMemoryStats(node.left);
        }
        if(node.right!=null){
            memUsage+=getMemoryStats(node.right);
        }
        return memUsage;
    }

    private String generateKey(String a, String b){
        return a+"##"+b;
    }
    

}