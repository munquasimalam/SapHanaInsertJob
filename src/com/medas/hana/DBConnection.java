package com.medas.hana;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class DBConnection {
	
		public static void main(String[] argv) throws ClassNotFoundException, SQLException {
			insertDataInHanaDbJob();
		}
		
		
		public static String readFileAsString(String fileName)throws Exception
		  {
		    String data = "";
		    data = new String(Files.readAllBytes(Paths.get(fileName)));
		    return data;
		  }
		
		/**
		 * Save the given text to the given filename.
		 * @param canonicalFilename Like /Users/al/foo/bar.txt
		 * @param text All the text you want to save to the file as one String.
		 * @throws IOException
		 */
		public static void writeLogToFile(String canonicalFilename, String text) 
		throws IOException
		{
			creatFile(canonicalFilename);
		  File file = new File (canonicalFilename);
		  BufferedWriter out = new BufferedWriter(new FileWriter(file)); 
		  out.write(text);
		  out.close();
		}
		
		public static void creatFile(String path) {
			 try {
			     File file = new File(path);
			     /*If file gets created then the createNewFile() 
			      * method would return true or if the file is 
			      * already present it would return false
			      */
		             boolean fvar = file.createNewFile();
			     if (fvar){
			          System.out.println("File has been created successfully");
			     }
			     else{
			          System.out.println("File already present at the specified location");
			     }
		    	} catch (IOException e) {
		    		System.out.println("Exception Occurred:");
			        e.printStackTrace();
			  }
		   }
		
		private static Connection getConnection() {
			Connection connection = null;
			try {
			   Class.forName("com.sap.db.jdbc.Driver");
				connection = DriverManager.getConnection("jdbc:sap://192.168.0.151:39015/?autocommit=false&currentschema=ECLINIC_HMC&user=SYSTEM&password=Medteam2013");
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
			 catch (SQLException e) {
				e.printStackTrace();
			}
			return connection;
		}
		public  static void insertDataInHanaDbJob() throws SQLException {

			Connection connection = null;
			Statement stmt =null;
			
			try{
				connection=getConnection();
			     stmt = connection.createStatement();
   
			  String basePath="C:/alam/sql/";
			   //String[] tables={"purchaseorder_flow","purchase_master","purchase_grn_master","procedure_setup_office","procedure_setup","patient_history","patient_examchild","new_registration","medicine_insurar_net","medicmine_setup","item_adjuststock","item_consumption","item_consumption_details","item_master","item_request_details","item_stock","labtest_insurar","labtest_insurar_net","medicine_insurar"};
			       String[] tables= {"procedure_insurar"};
			       for(String table:tables) {
			    	   int successCount=0;
				       int failedCount=0;
				       int totalCount=0;
				       String errorText="";
			    
			       String data =readFileAsString(basePath+table+".sql");
			       //String data =readFileAsString(basePath+"appointments.sql");
			       String queries[]=data.split(";"); 
			      for(String query:queries) {
			    	  try {
			    	  if(query.length()>2) {
			    		   stmt.executeUpdate(query+";"); 
				    	  System.out.println(query+";"); 
				    	  successCount++;
				    	  }
		                } catch(Exception e) {
		                	errorText+=e.getMessage()+query+";";
		                	  failedCount++;
		                	  e.printStackTrace();
		                	continue;   
					       }
			    	  
			        }
			       System.out.println(basePath+table+":SuccessCount:"+successCount);
			      errorText+=basePath+table+":  SuccessCount  :"+successCount;
			      System.out.println(basePath+table+":FailedCount:"+failedCount);
			      errorText+=basePath+table+":  FailedCount  :"+failedCount;
			      totalCount=successCount+failedCount;
			      System.out.println(basePath+table+"TotalCount:"+totalCount);
			      errorText+=basePath+table+":   TotalCount  :"+totalCount;
			      writeLogToFile(basePath+table+".log.txt ",errorText);
			       }
			       System.out.println("Done");
			      connection.commit();
			}catch(SQLException e){
			      e.printStackTrace();
			      connection.rollback();
			}catch(Exception e){
			      e.printStackTrace();
			      connection.rollback();
			
			} finally{
			      if(stmt!=null)
			      stmt.close();
			if(connection!=null)
			       connection.close();
			}
			
		}
		
		}
		
		

		