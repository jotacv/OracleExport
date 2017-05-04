package com.jotacv.utils;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Export {
	
	private static Connection connection = null; 
	
	private enum DataType{BLOB, LOB, DATE, NUMBER, OTH, NULL}
	
	private String daPattern = "(\\d\\d\\d\\d)-(\\d\\d)-(\\d\\d)\\s(\\d\\d):(\\d\\d):(\\d\\d)(\\.?\\d*)";
	
	private class RollingFileOutputStream {
		
		private int fileIdx = 1;		
		private int cc = 0;		
		private FileOutputStream file = null;
		private int rollCount = 50000;

		public RollingFileOutputStream(int rollCount) throws FileNotFoundException {
			this.rollCount = rollCount;
			this.file = new FileOutputStream("output0.sql");
		}
		
		private void rollFile() throws IOException{
			this.file.close();
			this.file= new FileOutputStream("output"+fileIdx+++".sql");
			this.cc = 0;	
		}
		
		public void write(byte[] b) throws IOException{
			if(cc++>rollCount)
				this.rollFile();
			this.file.write(b);
		}
		
		public void close() throws IOException{
			this.file.close();
		}
		
	}
	
	public static String byteToHex(byte[] bs) {
		StringBuilder sb = new StringBuilder(bs.length);
		for(byte b : bs){
			sb.append(String.format("%02X", b));
		}
		return sb.toString();
	}
	
	public boolean isLOB(String type){
		if (type!=null && 
				type.contains("CLOB") || 
				type.contains("NCLOB") ||
				type.contains("BFILE")){
			return true;
			
		}else{
			return false;
		}
	}
	
	public boolean isBLOB(String type){
		if (type!=null && type.contains("BLOB")){
			return true;
		}else{
			return false;
		}
	}
	public boolean isNumber(String type){
		if (type!=null && type.contains("NUMBER")){
			return true;
		}else{
			return false;
		}
	}	
	public boolean isDate(String type){
		if (type!=null && 
				type.contains("TIMESTAMP") || 
				type.equals("DATE")){
			return true;
			
		}else{
			return false;
		}
	}

	
	private class Data{
		public DataType type;
		public String val;
		
		public Data(String  type, String val){
			if (val!=null){
				if (isDate(type)){
					this.type=DataType.DATE;
					this.val= val;
				}else if(isLOB(type)){
					this.type = DataType.LOB;
					this.val=val;				
				}else if(isBLOB(type)){
					this.type = DataType.BLOB;
					this.val=val;
				}else if(isNumber(type)){
					this.type = DataType.NUMBER;
					this.val=val;
				}else{
					this.type=DataType.OTH;
					this.val = val;
				}
			}else{
				this.type= DataType.NULL;
				this.val = null;
			}
		}
		
		@Override
		public String toString(){
			String ret = null;
			switch (this.type){
			case LOB:
				ret = "q'{"+this.val+"}'";
				break;
			case BLOB:
				ret = "hextoraw('"+this.val+"')";
				break;
			case DATE:
				Matcher m = Pattern.compile(daPattern).matcher(this.val);
				if(m.matches()){
					ret= new StringBuilder()
							.append("TO_DATE('")
							.append(m.group(1)).append("-")
							.append(m.group(2)).append("-")
							.append(m.group(3)).append(" ")
							.append(m.group(4)).append(":")
							.append(m.group(5)).append(":")
							.append(m.group(6)).append("', 'YYYY-MM-DD HH24:MI:SS')").toString();
				}else{
					//Default, it will blow up in the insert but no data is lost and u can manually fix it
					ret = this.val;
					System.out.println("WARN: DEFAULTED DATE: "+ret);
				}
				break;
			case NULL:
				ret = "null";
				break;
			case NUMBER:
				ret = this.val;
				break;
			case OTH:
				ret = "'"+this.val.replace("'", "''")+"'";
				break;
			}
			return ret;
		}
	}
	
	private String createSqlPart1(String tableName, List<String> columns){
		StringBuilder ret = new StringBuilder(); 
		ret.append("INSERT INTO "+tableName+" (");
		for(int i=0;i<columns.size();i++){
			ret.append(columns.get(i));
			if(i!=columns.size()-1)
				ret.append(", ");
			else
				ret.append(") VALUES ");
		}
		return ret.toString();
	}
	
	private String createSqlPart2(List<Data> data){
		StringBuilder ret = new StringBuilder(); 
		ret.append("(");
		for(int i=0;i<data.size();i++){
			ret.append(data.get(i));
			if(i!=data.size()-1)
				ret.append(", ");
			else
				ret.append(");\n");
		}
		return ret.toString();
	}

	private List<String> findTablesByFilter(String owner, List<String> tablesFilter, boolean excludeFlag) throws SQLException{
		Statement statm=null;
		ResultSet res=null;
		List<String> tables=null;
		
		statm = connection.createStatement();
		res = statm.executeQuery("SELECT table_name FROM all_tables WHERE owner = '"+owner+"'");
		tables = new ArrayList<String>();
		while(res.next()){
			tables.add(res.getString(1));
		}
		res.close(); statm.close();
		System.out.println("\nGot "+tables.size()+" tables.");
		//Filter tables
		if(tablesFilter!=null && !tablesFilter.isEmpty()){
			List<String> tablesFiltered = new ArrayList<String>();
			for (String tableFilter : tablesFilter){
				if (tables.contains(tableFilter)^excludeFlag)
					tablesFiltered.add(tableFilter);
			}
			if(!tablesFiltered.isEmpty())
				tables = tablesFiltered;
			System.out.println("Narrowed down to "+tables.size());
		}
		return tables;
	}

	
	private void ExportAll(String owner, List<String> tablesFilter, boolean excludeFlag, boolean orderFlag) throws SQLException, IOException{
		
		//Tables list & order
		Statement statm=null;
		ResultSet res=null;
		List<String> tables=null;
		try{
			if(orderFlag){
				OrderFinder orderFinder = new OrderFinder(connection, owner, tablesFilter, excludeFlag);
				tables = orderFinder.getOrderedList();
			}else{
				System.out.print("  Using input order.");
				tables = findTablesByFilter(owner, tables, orderFlag);
			}
		}catch(Exception e){
			System.out.println(" ERROR: Cannot get tables order");
			tables = findTablesByFilter(owner, tables, orderFlag);
		}
		
		//Retrieving data & export
		String columnType = "";
		String columnContent = "";
		RollingFileOutputStream rfos = new RollingFileOutputStream(50000);
		int tableidx=0;
		for(String tableName: tables){
			tableidx++;
			try{
				List<String> columnsHeaders = new ArrayList<String>();
				System.out.println("Fetching table "+tableName);
				statm = connection.createStatement();
				res = statm.executeQuery("select COLUMN_NAME from USER_TAB_COLUMNS where table_name='"+tableName+"' order by column_id");
				while(res.next()){
					columnsHeaders.add(res.getString(1));
				}
				res.close(); statm.close();
				String insert_part1 = createSqlPart1(tableName,columnsHeaders);
				statm = connection.createStatement();
				res = statm.executeQuery("select * FROM "+tableName);
				ResultSetMetaData rmd = res.getMetaData();
				while(res.next()){
					List<Data> row = new ArrayList<Data>();
					for(int j=1;j<=columnsHeaders.size();j++){
						try{
							columnType 		= rmd.getColumnTypeName(j);
							columnContent	= null;
							if(rmd!=null && isLOB(columnType)){
								if(res.getClob(j)!=null){
									Clob clob = res.getClob(j);
									columnContent = clob!=null?clob.getSubString(1,(int)clob.length()):"";
									row.add(new Data(columnType,columnContent));
								}else{
									columnContent="";
									row.add(new Data(columnType,null));
								}
							}else if(rmd!=null && isBLOB(columnType)){
								Blob blob = res.getBlob(j);
								columnContent = blob!=null?byteToHex(blob.getBytes(1l,(int)blob.length())):"";
								row.add(new Data(columnType,columnContent));
							}else{
								columnContent=res.getString(j);
								row.add(new Data(columnType,columnContent));
							}
						}catch(Exception e){
							System.out.print("- ERROR IN TABLE "+tableName+",ROW "+tableidx+", COLUMN "+j+": ");
							e.printStackTrace();
							System.out.println("-- ColumnType: "+columnType);
							System.out.println("-- ColumnContent: "+columnContent);
						}
					}
					String insert_part2 = createSqlPart2(row);
					rfos.write((insert_part1+insert_part2).getBytes());
				}
			}catch(Exception e){
				System.out.print("- ERROR IN TABLE "+tableName+": ");
				e.printStackTrace();
				System.out.println("-- ColumnType: "+columnType);
				System.out.println("-- ColumnContent: "+columnContent);
			}finally{
				res.close();
				statm.close();
			}
			rfos.write("COMMIT;\n\n".getBytes());
		}
		rfos.close();
	}

	public static void main(String[] argv) throws Exception {
		System.out.println("------ Oracle JDBC Database Export ------");
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
		} catch (ClassNotFoundException e) {
			System.out.println("Where is your Oracle JDBC Driver?");
			e.printStackTrace();
			return;
		}

		

		List<String> listaTablas = null;
		boolean excludeFlag = false;
		boolean orderFlag = true;
		try {
			if (argv.length==3 || argv.length==4 || argv.length==5 ){
				connection = DriverManager.getConnection(argv[0],argv[1],argv[2]);
				if(argv.length>=4){
					String list = null;
					
					
					if(argv.length==4){
						list = argv[3];
					}else{
						list = argv[4];
						if(argv[3].equals("-e")){
							excludeFlag = true;
						}else if(argv[3].equals("-o")){
							orderFlag=false;
						}else{
							throw new IllegalArgumentException();
						}
						
					}
					
					try{
						listaTablas = Arrays.asList(list.replace(" ", "").split(","));
					}catch(Exception e){
						System.out.println("Cannot parse tables list. Make sure 4th argument is a comma separated list of tables");
					}
				}
			}else{
				throw new IllegalArgumentException("Usage: java -jar this.jar url user password [-e/-o] [tableList]");
			}
		} catch (SQLException e) {
			System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
			return;
		}

		if (connection != null) {
			System.out.println("Oracle JDBC Driver Connected!");
			Locale.setDefault(Locale.US);
			Export main = new Export();
			main.ExportAll(argv[1],listaTablas,excludeFlag, orderFlag);
			System.out.println("----------------- DONE! -----------------");
			connection.close();
		} else {
			System.out.println("Failed to make connection!");
		}
	}

}