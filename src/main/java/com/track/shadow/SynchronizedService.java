package com.track.shadow;

import java.io.File;
import java.io.IOException;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Iterator;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

public class SynchronizedService {
	int time;
	String projectId;
	SqlService sqlService;
	DBHelper source;
	DBHelper target;
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
			source = new DBHelper(source0.element("url").getData().toString(),
					source0.element("diver").getData().toString(), source0.element("user").getData().toString(),
					source0.element("password").getData().toString());

			Element target0 = rootElement.element("target");
			target = new DBHelper(target0.element("url").getData().toString(),
					target0.element("diver").getData().toString(), target0.element("user").getData().toString(),
					target0.element("password").getData().toString());

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

			sqlService = new SqlService();

			time = Integer.valueOf(rootElement.element("time").getData().toString());
			projectId = rootElement.element("projectId").getData().toString();

			LogServer.log("end to init configure...");
		} catch (IOException e) {
			e.printStackTrace();
		} catch (DocumentException e) {
			e.printStackTrace();
		}
	}

	public void process() {
		int index = 0;
		while (true) {
			init();
			LogServer.log("start to update tables...");
			ArrayList<Table> tmpTables = new ArrayList<>();
			tmpTables.addAll(tables);
			final int t = index;
			tmpTables.forEach(table -> {
				if (t % table.timeOff != 0) {
					return;
				}
				LogServer.log("start to init table...table = " + table.name);
				if (table.type.equals("update")) {
					table.max = 0;
					LogServer.log("this table must update data all of this...");
				} else if (table.type.equals("insert")) {
					sqlService.getLast(target.getConn(), table, projectId);
					LogServer.log("this table only insert...insert from " + table.max);
				}

				LogServer.log("start to get data from source...");
				sqlService.getRecords(source.getConn(), table);
				LogServer.log("get data from source completed...data count = " + table.recods.size());

				LogServer.log("start to update data to target...");
				sqlService.updateTable(target.getConn(), table, projectId);
				LogServer.log("update data to target completed...");
				LogServer.log(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
			});
			LogServer.log("####################################################");
			index++;
			source.close();
			target.close();
			try {
				Thread.sleep(time * 1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
