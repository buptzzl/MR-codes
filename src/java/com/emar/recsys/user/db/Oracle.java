/**
 * @desc 通用数据库连接， 采用单例模式
 * @author emar.zlm
 * @ref http://www.eygle.com/digest/2008/05/java_oracle_procedure.html
 * @ref http://sunday132.iteye.com/blog/623439
 * 
 */

package com.emar.recsys.user.db;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.dbcp.BasicDataSource;

public class Oracle {

	private static String dbUrl = "jdbc:oracle:thin:@221.122.127.47:1521:gwkpro";
	private static String dbDrive = "oracle.jdbc.driver.OracleDriver";
	private static String dbUser = "DBO_GOUWUKE_CHS";
	private static String dbPwd = "DBO_GOUWUKE_CHS";

	private Oracle() {
	}; // No instance.

	public static Dbcp DBService = null;

	public static Dbcp getInstance() {
		if (DBService == null) {
			DBService = new Dbcp();
			DBService.initDataSource();
		}

		return DBService;
	}

	public static String getDbUrl() {
		return dbUrl;
	}

	public static String getDbDrive() {
		return dbDrive;
	}

	public static String getDbUser() {
		return dbUser;
	}

	public static String getDbPwd() {
		return dbPwd;
	}

	public static boolean setDbUrl(String s) {
		if (s == null) {
			return false;
		}
		dbUrl = s;
		return true;
	}

	public static boolean setDbDrive(String s) {
		if (s == null) {
			return false;
		}
		dbDrive = s;
		return true;
	}

	public static boolean setDbUser(String s) {
		if (s == null) {
			return false;
		}
		dbUser = s;
		return true;
	}

	public static boolean setDbPwd(String s) {
		if (s == null) {
			return false;
		}
		dbPwd = s;
		return true;
	}

	/**
	 * 使用DBCP 1.4 做数据库连接池 DBCP1.4支持JDK1.6
	 */
	public static class Dbcp {
		private BasicDataSource dataSource = null;

		// 初始化数据连接
		public void initDataSource() {
			if (dataSource != null) {
				try {
					dataSource.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
				dataSource = null;
			}

			try {
				dataSource = new BasicDataSource();
				dataSource.setDriverClassName(Oracle.dbDrive);
				dataSource.setUrl(Oracle.dbUrl);
				dataSource.setUsername(Oracle.dbUser);
				dataSource.setPassword(Oracle.dbPwd);
				dataSource.setMaxActive(20);
				dataSource.setMaxIdle(10);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		// 从连接池中获得数据库连接
		public Connection getConnection() throws SQLException {
			if (dataSource != null) {
				return dataSource.getConnection();
			} else {
				throw new SQLException("数据库联连接失败");
			}
		}

		public void disconnect() {
			if (dataSource == null) {
				return;
			}
			try {
				if (dataSource.getConnection() != null) {
					try {
						dataSource.getConnection().close();
					} catch (SQLException e) {
						e.printStackTrace();
						System.err.print("Oracle dataSouce connection close.");
					}
				}
				dataSource.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * @func test 执行预编译SQL语句
	 * @throws SQLException
	 */
	static void preparedStatement() throws SQLException {
		// SQL 语句被预编译并且存储在 PreparedStatement 对象中。然后可以使用此对象高效地多次执行该语句。
		Connection conn = Oracle.getInstance().getConnection();
		PreparedStatement prepStmt = conn
				.prepareStatement("select * from gwkproduct where id=14195222");
		ResultSet set = prepStmt.executeQuery();
		while (set.next()) {
			System.out.print(" " + set.getInt("id") + " "
					+ set.getString("productname") + " "
					+ set.getString("pinpai"));
		}
		set.close();
		prepStmt.close();
		conn.close();
	}

	/**
	 * Java调用Oracle的存储过程
	 */
	static void callableSatement() {
		Connection con = null;
		CallableStatement callStmt = null;

		try {
			con = Oracle.getInstance().getConnection();
			System.out.println("创建连接成功");
			// 调用Oralce的存储过程luketest(?)
			callStmt = con.prepareCall("{ call HYQ.TESTA(?,?) }"); // ("BEGIN luketest(?); END;");
			callStmt.setInt(1, 682);
			System.out.println("调用Oralce的存储过程");

			callStmt.execute(); /* 如果这里阻塞说明上面的store procedure正被独占访问/或者事务没有提交 */
			System.out.println("存储过程执行成功");
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (callStmt != null)
					callStmt.close();
				if (con != null)
					con.close();
			} catch (SQLException ex) {
				ex.printStackTrace();
			}
		}
	}

	/**
	 * 执行SQL
	 */
	static void statement() {
		// 执行大量的查询语句
		for (int i = 0; i < 100; i++) {
			Connection conn = null;
			Statement stmt = null;
			ResultSet set = null;
			try {
				conn = Oracle.getInstance().getConnection();
				stmt = conn.createStatement();
				set = stmt
						.executeQuery("select * from account_info where account_id=5000007");
				while (set.next()) {
					System.out.print(i + " " + set.getInt("account_id"));
					System.out.print(" " + set.getString("account_name"));
					System.out.println(" " + set.getString("account_password"));
				}
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				try {
					if (set != null)
						set.close();
					if (stmt != null)
						stmt.close();
					if (conn != null)
						conn.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			Oracle.preparedStatement();
			Oracle.getInstance().disconnect();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
