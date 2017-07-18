package com.track.shadow;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SqlService {
	public static String PAGESIZE = "1000";

	private static final String MYSQL = "mySql";
	private static final String SQLSERVER = "sqlServer";

	public static SqlService instanceMySql = new SqlService(MYSQL);
	public static SqlService instanceSqlServer = new SqlService(SQLSERVER);

	private SqlService(String divier) {
		this.divier = divier;
	}

	private String divier;

	public static SqlService instanceMySql() {
		return instanceMySql;
	}

	public static SqlService instanceSqlServer() {
		return instanceSqlServer;
	}

	public void getLast(Connection conn, Table table, String projctId) {
		String sql = "SELECT MAX(id) AS last FROM " + projctId + table.name;
		try (PreparedStatement prepareStatement = conn.prepareStatement(sql);
				ResultSet rs = prepareStatement.executeQuery()) {
			if (rs != null && rs.next()) {
				table.max = rs.getInt("last");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public boolean getRecords(Connection conn, Table table, boolean flg) {
		if (table.max == -1) {
			return false;
		}
		if (table.properties.isEmpty()) {
			return false;
		}

		table.recods.clear();
		try {
			StringBuilder sql = new StringBuilder();
			sql.append("SELECT ");

			if (flg && divier.equals(SQLSERVER)) {
				sql.append(" TOP " + PAGESIZE + " ");
			}

			for (Property p : table.properties) {
				sql.append(p.name + ", ");
			}
			sql.deleteCharAt(sql.length() - 2);
			sql.append("FROM ");
			sql.append(table.name + " ");
			sql.append("WHERE id>" + table.max);

			if (flg && divier.equals(MYSQL)) {
				sql.append(" LIMIT " + PAGESIZE + " ");
			}

			PreparedStatement prepareStatement = conn.prepareStatement(sql.toString());
			ResultSet rs = prepareStatement.executeQuery();
			int size = 0;
			while (rs.next() && (flg ? size < Integer.valueOf(PAGESIZE) : true)) {
				Recod rec = new Recod();
				for (Property p : table.properties) {
					if (p.type.equals(String.class)) {
						String value = rs.getString(p.name);
						if (value != null) {
							rec.add(p.name, value, String.class, p.dataSourceName);
						}
					} else if (p.type.equals(Integer.class)) {
						Integer value = rs.getInt(p.name);
						if (value != null) {
							rec.add(p.name, value, Integer.class, p.dataSourceName);
						}
					} else if (p.type.equals(Date.class)) {
						Date value = rs.getDate(p.name);
						if (value != null) {
							rec.add(p.name, value, Date.class, p.dataSourceName);
						}
					}
				}
				table.add(rec);
				size++;
			}
			if (size == Integer.valueOf(PAGESIZE)) {
				return true;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return false;
	}

	public void updateTable(Connection conn, Table table, String projectId) {
		if (table.properties.isEmpty()) {
			return;
		}
		if (table.type.equals("update")) {
			StringBuilder sql = new StringBuilder();
			sql.append("truncate table " + projectId + table.name);
			try (PreparedStatement prepareStatement = conn.prepareStatement(sql.toString());) {
				prepareStatement.execute();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		StringBuilder sql = new StringBuilder();
		sql.append("insert into " + projectId + table.name + " (");
		for (Property p : table.properties) {
			sql.append(p.name + ", ");
		}
		sql.deleteCharAt(sql.length() - 2);
		sql.append(") values (");
		for (@SuppressWarnings("unused")
		Property p : table.properties) {
			sql.append("?, ");
		}
		sql.deleteCharAt(sql.length() - 2);
		sql.append(")");

		try (PreparedStatement prepareStatement = conn.prepareStatement(sql.toString());) {
			for (Recod rec : table.recods) {
				for (int i = 0; i < table.properties.size(); i++) {
					Property p = table.properties.get(i);
					Property property = rec.map.get(p.name);
					if (property == null) {
						continue;
					}
					if (p.type.equals(String.class)) {
						prepareStatement.setString(i + 1, (String) (property.value));
					} else if (p.type.equals(Integer.class)) {
						prepareStatement.setInt(i + 1, (int) (property.value));
					} else if (p.type.equals(Date.class)) {
						prepareStatement.setDate(i + 1, (Date) (property.value));
					}
				}
				prepareStatement.addBatch();
			}
			prepareStatement.executeBatch();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
