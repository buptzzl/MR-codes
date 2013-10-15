/**
 * @func ���Ի����ݿ�����
 * 
 */
package com.emar.recsys.user.services;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;


public class DatabaseTest {
	
	

	/**
	 * @param args
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws SQLException, ClassNotFoundException {
		Class.forName("oracle.jdbc.driver.OracleDriver");
		Connection conn = DriverManager.getConnection(
				"jdbc:oracle:thin:@221.122.127.47:1521:gwkpro",
				"DBO_GOUWUKE_CHS", "DBO_GOUWUKE_CHS");
		
		PreparedStatement prepStmt = conn.prepareStatement("select * from gwkproduct where id=14194336");
		ResultSet set = prepStmt.executeQuery();
        while (set.next()) {
              System.out.print(" " + set.getInt("id"));
              System.out.print(" " + set.getString("productname"));
              System.out.println(" " + set.getString("pinpai"));
        }
		
	}

}
