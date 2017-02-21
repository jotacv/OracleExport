package com.jotacv.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OrderFinder {
	
	private boolean ordered = false;
	private SwappingList orderedList = null;
	private Map<String,Integer> swapMap = new HashMap<String,Integer>();
	
	public boolean isOrdered(){return ordered;}
	public List<String> getOrderedList(){
		if(ordered){
			List<String> ret = new ArrayList<String>();
			for(String table : orderedList){
				ret.add(table);
			}
			return ret;
		}else{
			return null;
		}
	}
	
	private String constraintQuery = "SELECT ucc1.TABLE_NAME, ucc2.TABLE_NAME "
			+ "FROM user_constraints uc, user_cons_columns ucc1, user_cons_columns "
			+ "ucc2 WHERE uc.constraint_name = ucc1.constraint_name AND uc.r_constraint_name "
			+ "= ucc2.constraint_name AND ucc1.POSITION = ucc2.POSITION AND uc.constraint_type "
			+ "= 'R'AND uc.constraint_name = '";
	
	private String constraintLookupQuery = "select c.constraint_name from all_constraints c, all_tables t "
			+ "where c.table_name = t.table_name AND t.owner = '";
	
	public static Connection getConnection(String url, String user, String password) throws SQLException{
		return DriverManager.getConnection(url,user,password);
	}
	
	private class SwappingList extends ArrayList<String>{		
		private static final long serialVersionUID = 1L;
		public void moveDown(String tableName){
			int i = this.indexOf(tableName);
			Collections.swap(this,i,i+1);
		}
		public void moveUp(String tableName){
			int i = this.indexOf(tableName);
			Collections.swap(this,i,i-1);
		}
		public SwappingList(){
			super();
		}
	}

	public class Constraint{
		public String tableFrom;
		public String tableTo;
		public Constraint(String tableFrom, String tableTo){
			this.tableFrom=tableFrom;
			this.tableTo=tableTo;
		}
	}
	
	public OrderFinder(Connection connection, String owner, List<String> tablesFilter) throws SQLException{
		//First get all the tables or filtered
		System.out.print("Tables lookup");
		int i =0;
		Statement statm = connection.createStatement();
		ResultSet res = statm.executeQuery("SELECT table_name FROM all_tables WHERE owner = '"+owner+"'");
		List<String> tables = new ArrayList<String>();
		while(res.next()){
			if((i++%10)==0)System.out.print(".");
			tables.add(res.getString(1));
		}
		res.close();
		statm.close();
		System.out.println(" Got "+tables.size()+" tables.");
		if(tablesFilter!=null && !tablesFilter.isEmpty()){
			List<String> tablesFiltered = new ArrayList<>();
			for (String table : tablesFilter){
				if (tables.contains(table))
					tablesFiltered.add(table);
			}
			if(!tablesFiltered.isEmpty())
				tables = tablesFiltered;
			System.out.println("Narrowed down to "+tables.size());
		}
		
		//Second get all the constraints
		System.out.print("Constraint lookup");
		Statement statm2 = connection.createStatement();
		String filterTablesOnConstraintLookup = "";
		if(tablesFilter!=null && !tablesFilter.isEmpty()){
			StringBuilder filterTablesOnConstraintLookupBuilder = new StringBuilder();
			filterTablesOnConstraintLookupBuilder.append("and t.table_name in (");
			for (String table : tablesFilter){
				filterTablesOnConstraintLookupBuilder.append("'").append(table).append("'").append(",");
			}
			filterTablesOnConstraintLookupBuilder.deleteCharAt(filterTablesOnConstraintLookup.length()-1);
			filterTablesOnConstraintLookupBuilder.append(")");
			filterTablesOnConstraintLookup = filterTablesOnConstraintLookupBuilder.toString();
		}
		
		ResultSet res2 = statm2.executeQuery(constraintLookupQuery+owner+"'"+filterTablesOnConstraintLookup);
		List<Constraint> constraints = new ArrayList<Constraint>();
		String constraintName=null;
		i =0;
		while(res2.next()){
			if((i++%10)==0)System.out.print(".");
			constraintName=res2.getString(1);
			statm = connection.createStatement();
			res = statm.executeQuery(constraintQuery+constraintName+"'");
			while(res.next()){
				constraints.add(new Constraint(res.getString(1), res.getString(2)));
			}
			res.close();
			statm.close();
		}
		res2.close();
		statm2.close();
		System.out.println(" Got "+constraints.size()+" constraints.");		
		
		//Then do the magic
		System.out.print("Ordering");
		this.orderedList = new SwappingList();
		this.orderedList.addAll(tables);
		i = 0;
		int c = 0;
		int swapTimes = 0;
		int maxSwap = 1000;
		int maxSwap10 = maxSwap*10;
		Constraint constraint = null;
		for (String table: orderedList){
			swapMap.put(table, 0);
		}
		while(i<constraints.size()){
			if((++c%maxSwap10)==0)System.out.print(".");
			constraint = constraints.get(i);
			if(orderedList.indexOf(constraint.tableFrom)<orderedList.indexOf(constraint.tableTo)){
				swapTimes = swapMap.get(constraint.tableFrom);
				if(swapTimes>=maxSwap){
					i++;
				}else{
					orderedList.moveDown(constraint.tableFrom);
					orderedList.moveUp(constraint.tableTo);
					swapMap.put(constraint.tableFrom, ++swapTimes);
					i=0;
				}
			}else{
				i++;
			}
		}
		System.out.println(" Done");
		i=0;
		for (Constraint constr : constraints){
			if(orderedList.indexOf(constr.tableFrom)<orderedList.indexOf(constr.tableTo)){
				System.out.println("   WARN: Constraint broken "+(++i)+": "+constr.tableFrom+"->"+constr.tableTo);
			}
		}
		
		this.ordered=true;
			
	}
	
	public static void main(String[] argv) throws Exception {
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
		} catch (ClassNotFoundException e) {
			System.out.println("Where is your Oracle JDBC Driver?");
			e.printStackTrace();
			return;
		}
		if (argv.length==3){
			Connection connection = getConnection(argv[0],argv[1],argv[2]);
			if (connection!=null){
				System.out.println("Connected! -- Sorting tables algorithm");
				OrderFinder orderFinder = new OrderFinder(connection,argv[1],null);
				//Output
				for (String orderedTable : orderFinder.getOrderedList()){
					System.out.println(orderedTable);
				}
			}
		}else{
			throw new IllegalArgumentException("Usage: java -jar this.jar url user password");
		}
	}
}
