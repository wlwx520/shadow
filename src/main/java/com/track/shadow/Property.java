package com.track.shadow;

public class Property {
	String name;
	Object value;
	Class<?> type;
	String dataSourceName;

	public Property(String name, Object value, Class<?> type, String dataSourceName) {
		super();
		this.name = name;
		this.value = value;
		this.type = type;
		this.dataSourceName = dataSourceName;
	}
}