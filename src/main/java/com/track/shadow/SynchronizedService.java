package com.track.shadow;

import java.io.File;
import java.io.IOException;
import java.sql.Date;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

public class SynchronizedService {
	boolean isRun;
	boolean stop;
	int time;
	String projectId;
	SqlService mySqlService;
	SqlService sqlServerService;
	DBHelper source;
	DBHelper target;
	String sourceDiver;
	String targetDiver;
	ArrayList<Table> tables;

	@SuppressWarnings("unchecked")
	public void init() {
		try {
			LogServer.log("start to init configure...");

			SAXReader saxReader = new SAXReader();
			File file = new File(
					new File("").getCanonicalPath() + File.separator + "config" + File.separator + "configure.xml");
			Document document = saxReader.read(file);
			Element rootElement = document.getRootElement();

			Element source0 = rootElement.element("source");
			String sourceDiver = source0.element("diver").getData().toString();
			this.sourceDiver = sourceDiver;
			source = new DBHelper(source0.element("url").getData().toString(), sourceDiver,
					source0.element("user").getData().toString(), source0.element("password").getData().toString());

			Element target0 = rootElement.element("target");
			String targetDiver = target0.element("diver").getData().toString();
			this.targetDiver = targetDiver;
			target = new DBHelper(target0.element("url").getData().toString(), targetDiver,
					target0.element("user").getData().toString(), target0.element("password").getData().toString());

			tables = new ArrayList<>();
			Element tables0 = rootElement.element("tables");
			Iterator<Element> tablesIt = tables0.elementIterator("table");
			while (tablesIt.hasNext()) {
				Element table0 = tablesIt.next();
				String tableName = table0.attribute("name").getData().toString();
				String tableType = table0.attribute("type").getData().toString();
				int timeOff = Integer.valueOf(table0.attribute("timeoff").getData().toString());
				if (!tableType.equals("update") && !tableType.equals("insert")) {
					throw new RuntimeException(
							"type is error...only support to be update or insert !  this table is " + tableName);
				}
				Iterator<Element> fieldsIt = table0.elementIterator("field");
				ArrayList<Property> properties = new ArrayList<>();
				while (fieldsIt.hasNext()) {
					Element field0 = fieldsIt.next();
					String fieldName = field0.attributeValue("name");
					String fieldType = field0.attributeValue("type");
					switch (fieldType) {
					case "int":
						properties.add(new Property(fieldName, null, Integer.class, fieldType));
						break;
					case "varchar":
					case "nvarchar":
						properties.add(new Property(fieldName, null, String.class, fieldType));
						break;
					case "Date":
						properties.add(new Property(fieldName, null, Date.class, fieldType));
						break;
					default:
						throw new RuntimeException(
								"field type is error...only support to be int String or Date !   this field is "
										+ fieldName);
					}
				}
				Table table = new Table(tableName, properties, tableType, timeOff);
				tables.add(table);
			}

			mySqlService = SqlService.instanceMySql();
			sqlServerService = SqlService.instanceSqlServer();

			time = Integer.valueOf(rootElement.element("time").getData().toString());
			projectId = rootElement.element("projectId").getData().toString();
			SqlService.PAGESIZE = rootElement.element("pageSize").getData().toString();

			LogServer.log("end to init configure...");
		} catch (IOException e) {
			e.printStackTrace();
		} catch (DocumentException e) {
			e.printStackTrace();
		}
	}

	private SqlService getSourceService() {
		switch (sourceDiver) {
		case "com.microsoft.sqlserver.jdbc.SQLServerDriver":
			return sqlServerService;
		case "com.mysql.jdbc.Driver":
			return mySqlService;
		}
		return null;
	}

	private SqlService getTargetService() {
		switch (targetDiver) {
		case "com.microsoft.sqlserver.jdbc.SQLServerDriver":
			return sqlServerService;
		case "com.mysql.jdbc.Driver":
			return mySqlService;
		}
		return null;
	}

	public void process() {
		try {
			this.isRun = true;
			int index = 0;
			while (!stop) {
				init();
				LogServer.log("start to update tables...");
				final int t = index;
				tables.forEach(table -> {
					if (t % table.timeOff != 0) {
						return;
					}
					LogServer.log("start to init table...table = " + table.name);
					if (table.type.equals("update")) {
						table.max = 0;
						LogServer.log("this table must update data all of this...");
						LogServer.log("start to get data from source...");

						getSourceService().getRecords(source.getConn(), table, false);
						LogServer.log("get data from source completed...data count = " + table.recods.size());

						LogServer.log("start to update data to target...");
						getTargetService().updateTable(target.getConn(), table, projectId);
						LogServer.log("update data to target completed...");
						LogServer.log(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
					} else if (table.type.equals("insert")) {
						boolean again = false;
						do {
							getTargetService().getLast(target.getConn(), table, projectId);
							LogServer.log("this table only insert...insert from " + table.max);

							LogServer.log("start to get data from source...");
							again = getSourceService().getRecords(source.getConn(), table, true);
							LogServer.log("get data from source completed...data count = " + table.recods.size());

							LogServer.log("start to update data to target...");
							getTargetService().updateTable(target.getConn(), table, projectId);
							LogServer.log("update data to target completed...");
							LogServer.log(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
						} while (again);
					}

				});
				LogServer.log("####################################################");
				index++;
				source.close();
				target.close();
				source = null;
				target = null;
				tables = null;
				Thread.sleep(time * 1000);
			}
		} catch (Exception e) {
			e.printStackTrace();
			try {
				if (target.getConn() != null && !target.getConn().isClosed()) {
					target.getConn().close();
				}
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			try {
				if (source.getConn() != null && !source.getConn().isClosed()) {
					source.getConn().close();
				}
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			this.isRun = false;
		}
	}

	public boolean isRun() {
		return isRun;
	}

	public void stop() {
		this.stop = true;
	}
}
