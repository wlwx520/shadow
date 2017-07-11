package com.track.shadow;

public class Property {
	String name;
	Object value;
	Class<?> type;

	public Property(String name, Object value, Class<?> type) {
		super();
		this.name = name;
		this.value = value;
		this.type = type;
	}
}