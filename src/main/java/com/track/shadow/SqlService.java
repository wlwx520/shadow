package com.track.shadow;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class SqlService {
	public static SqlService instance = new SqlService();

	public static SqlService instance() {
		return instance;
	}

	public void createTableIfNotExits(Connection conn, List<Table> tables, String projectId) {
		if (tables != null) {
			for (Table table : tables) {
				try {
					StringBuilder sql = new StringBuilder();
					sql.append("if not exists (select * from sysobjects where id = object_id('");
					sql.append(table.name + projectId);
					sql.append("') ");
					sql.append("and OBJECTPROPERTY(id, 'IsUserTable') = 1 ");
					sql.append("create table");
					sql.append(table.name + projectId);
					PreparedStatement prepareStatement = conn.prepareStatement(sql.toString());
					prepareStatement.execute();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public void getLast(Connection conn, Table table, String projctId) {
		String sql = "SELECT MAX(id) AS last FROM " + table.name + projctId;
		try (PreparedStatement prepareStatement = conn.prepareStatement(sql);
				ResultSet rs = prepareStatement.executeQuery()) {
			if (rs != null && rs.next()) {
				table.max = rs.getInt("last");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void getRecords(Connection conn, Table table) {
		if (table.max == -1) {
			return;
		}
		if (table.properties.isEmpty()) {
			return;
		}
		try {
			StringBuilder sql = new StringBuilder();
			sql.append("SELECT ");
			for (Property p : table.properties) {
				sql.append(p.name + ", ");
			}
			sql.deleteCharAt(sql.length() - 2);
			sql.append("FROM ");
			sql.append(table.name + " ");
			sql.append("WHERE id>" + table.max);
			PreparedStatement prepareStatement = conn.prepareStatement(sql.toString());
			ResultSet rs = prepareStatement.executeQuery();
			while (rs.next()) {
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
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void updateTable(Connection conn, Table table, String projectId) {
		if (table.properties.isEmpty()) {
			return;
		}
		if (table.type.equals("update")) {
			StringBuilder sql = new StringBuilder();
			sql.append("truncate table " + table.name + projectId);
			try (PreparedStatement prepareStatement = conn.prepareStatement(sql.toString());) {
				prepareStatement.execute();
  			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		StringBuilder sql = new StringBuilder();
		sql.append("insert into " + table.name + projectId + " (");
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
					if(property==null){
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
