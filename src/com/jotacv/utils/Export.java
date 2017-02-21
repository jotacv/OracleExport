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
	
	private enum DataType{LOB, DATE, NUMBER, OTH, NULL}
	
//	private int lobCount = 1;
	
	private String daPattern = "(\\d\\d\\d\\d)-(\\d\\d)-(\\d\\d)\\s(\\d\\d):(\\d\\d):(\\d\\d)(\\.?\\d*)";
	
	private int fileIdx= 0;
	
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
				}else if(isLOB(type) || isBLOB(type)){
					this.type = DataType.LOB;
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
//			try{
				switch (this.type){
				case LOB:
//					ret = "lob"+(lobCount++)+".txt";
//					FileOutputStream fos = new FileOutputStream(ret);
//					fos.write(this.val.getBytes());
//					fos.close();
//					ret="'["+ret+"]'";
					ret = "q'{"+this.val+"}'";
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
//			}catch(FileNotFoundException e){} catch (IOException e) {
//				System.out.println("Something happened with lob "+lobCount);
//			}
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
	
	private FileOutputStream getFile() throws FileNotFoundException{
		return new FileOutputStream("output"+fileIdx+++".sql");
	}

	
	private void ExportAll(String owner, List<String> tablesFilter, boolean excludeFlag) throws SQLException, IOException{
		//Tables list & order
		Statement statm=null;
		ResultSet res=null;
		List<String> tables=null;
		try{
			OrderFinder orderFinder = new OrderFinder(connection, owner, tablesFilter, excludeFlag);
			tables = orderFinder.getOrderedList();
		}catch(Exception e){
			System.out.println(" ERROR: Cannot get tables order");
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
				List<String> tablesFiltered = new ArrayList<>();
				for (String table : tables){
					if (tablesFilter.contains(table)^excludeFlag)
						tablesFiltered.add(table);
				}
				if(!tablesFiltered.isEmpty())
					tables = tablesFiltered;
				System.out.println("Narrowed down to "+tables.size());
			}
			System.out.println("\n");
		}
		

		
		//Retrieving data & export
		String columnType = "";
		String columnContent = "";
		FileOutputStream fos = getFile();
		int tableidx=0;
		int lineOutputCount = 0;
		for(String tableName: tables){
			tableidx++;
			fos.write(("\nREM INSERTING into "+tableName+"\n").getBytes());
			fos.write("SET DEFINE OFF;\n".getBytes());
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
								columnContent = blob!=null?blob.toString():"";
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
					fos.write((insert_part1+insert_part2).getBytes());
					lineOutputCount++;
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
			fos.write("COMMIT;\n\n".getBytes());
			//When 10k insert lines are reached change file
			if(lineOutputCount>=10000){
				lineOutputCount=0;
				fos.close();
				fos = getFile();
			}
		}
		fos.close();
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

		System.out.println("Oracle JDBC Driver Connected!");

		List<String> listaTablas = null;
		boolean excludeFlag = false;
		try {
			if (argv.length==3 || argv.length==4 || argv.length==5){
				connection = DriverManager.getConnection(argv[0],argv[1],argv[2]);
				if(argv.length>=4){
					String list = null;
					
					if(argv.length==4){
						list = argv[3];
					}else{
						excludeFlag = argv[3].equals("-e");
						if(!excludeFlag)
							throw new IllegalArgumentException();
						list = argv[4];
					}
					try{
						listaTablas = Arrays.asList(list.replace(" ", "").split(","));
					}catch(Exception e){
						System.out.println("Cannot parse tables list. Make sure 4th argument is a comma separated list of tables");
					}
				}
			}else{
				throw new IllegalArgumentException("Usage: java -jar this.jar url user password [-e] [tableList]");
			}
		} catch (SQLException e) {
			System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
			return;
		}

		if (connection != null) {
			Locale.setDefault(Locale.US);
			Export main = new Export();
			main.ExportAll(argv[1],listaTablas,excludeFlag);
			System.out.println("----------------- DONE! -----------------");
		} else {
			System.out.println("Failed to make connection!");
		}
	}

}