package com.track.shadow;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBHelper {
	public String url;
	public String diver;
	public String user;
	public String password;

	public Connection conn = null;

	public DBHelper(String url, String diver, String user, String password) {
		try {
			this.url = url;
			this.diver = diver;
			this.user = user;
			this.password = password;
			Class.forName(diver);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public Connection getConn() {
		try {
			if (conn == null || conn.isClosed()) {
				conn = DriverManager.getConnection(url, user, password);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return conn;
	}

	public void close() {
		try {
			if (conn != null && !conn.isClosed()) {
				conn.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}