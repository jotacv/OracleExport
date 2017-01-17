package jotacv;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OrderFinder {
	
	private boolean ordered = false;
	private SwappingList orderedList = null;
	
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
	
	public static Connection getConnection(String url, String user, String password) throws SQLException{
		return DriverManager.getConnection(url,user,password);
	}
	
	private class SwappingList extends ArrayList<String> implements Comparator<String>{		
		private static final long serialVersionUID = 1L;
		private Map<String,Integer> swapMap = new HashMap<String, Integer>();
		private int maxSwap = 1000;
		private int i = 0;
		private List<Constraint> constraints;
		private Constraint getConstraint(String tableFrom, String tableTo){
			for(Constraint constraint : constraints){
				if (constraint.tableFrom.equals(tableFrom) && constraint.tableTo.equals(tableTo)){
					return constraint;
				}
			}
			return null;
		}
		@Override
		public int compare(String tableFrom, String tableTo){
			if((this.i++%10)==0)System.out.print(".");
			Constraint constraint = getConstraint(tableFrom, tableTo);
			if(constraint==null){
				return 0;
			}else{
				if(this.indexOf(tableFrom)<this.indexOf(tableTo)){
					String key = (String) (tableFrom+"-"+tableTo);
					if(!swapMap.keySet().contains(key))
						swapMap.put(key, 0);
					int val = swapMap.get(key);
					if (val>=maxSwap)
						return -1;
					swapMap.put(key, val++);
					if (val%100 == 99)
						System.out.println("WARN: Key '"+key+"' exceded "+val+" iterations");
					return 1;
				}else{
					return -1;
				}
			}
		}
		public void setConstraints(List<Constraint> constraints) {
			this.constraints = constraints;
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
	
	public OrderFinder(Connection connection, String owner) throws SQLException{
		//First get all the tables
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
		
		//Second get all the constraints
		System.out.print("Constraint lookup");
		Statement statm2 = connection.createStatement();
		ResultSet res2 = statm2.executeQuery("select c.constraint_name from all_constraints c, all_tables t "
				+ "where c.table_name = t.table_name AND t.owner = '"+owner+"'");
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
		this.orderedList.setConstraints(constraints);
		this.orderedList.addAll(tables);
		Collections.sort(this.orderedList);
		this.ordered=true;
		System.out.println(" Done");	
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
				OrderFinder orderFinder = new OrderFinder(connection,argv[1]);
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
