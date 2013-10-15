/**
 * @desc ͨ����ݿ����ӣ� ���õ���ģʽ
 * @author emar.zlm
 * @ref http://www.eygle.com/digest/2008/05/java_oracle_procedure.html
 * @ref http://sunday132.iteye.com/blog/623439
 * 
 */

package com.emar.recsys.user.services;

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
	 * ʹ��DBCP 1.4 ����ݿ����ӳ� DBCP1.4֧��JDK1.6
	 */
	public static class Dbcp {
		private BasicDataSource dataSource = null;

		// ��ʼ���������
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

		// �����ӳ��л����ݿ�����
		public Connection getConnection() throws SQLException {
			if (dataSource != null) {
				return dataSource.getConnection();
			} else {
				throw new SQLException("��ݿ�������ʧ��");
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
	 * @func test ִ��Ԥ����SQL���
	 * @throws SQLException
	 */
	static void preparedStatement() throws SQLException {
		// SQL ��䱻Ԥ���벢�Ҵ洢�� PreparedStatement �����С�Ȼ�����ʹ�ô˶����Ч�ض��ִ�и���䡣
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
	 * Java����Oracle�Ĵ洢���
	 */
	static void callableSatement() {
		Connection con = null;
		CallableStatement callStmt = null;

		try {
			con = Oracle.getInstance().getConnection();
			System.out.println("�������ӳɹ�");
			// ����Oralce�Ĵ洢���luketest(?)
			callStmt = con.prepareCall("{ call HYQ.TESTA(?,?) }"); // ("BEGIN luketest(?); END;");
			callStmt.setInt(1, 682);
			System.out.println("����Oralce�Ĵ洢���");

			callStmt.execute(); /* �����������˵�������store procedure���ռ����/��������û���ύ */
			System.out.println("�洢���ִ�гɹ�");
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
	 * ִ��SQL
	 */
	static void statement() {
		// ִ�д����Ĳ�ѯ���
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
