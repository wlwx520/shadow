package com.track.shadow;

import java.util.ArrayList;

public class Table {
	String type;
	String name;
	ArrayList<Property> properties;
	int max = -1;
	ArrayList<Recod> recods = new ArrayList<>();

	public Table(String name, ArrayList<Property> properties, String type) {
		this.name = name;
		this.properties = properties;
		this.type = type;
	}

	public void add(Recod rec) {
		this.recods.add(rec);
	}
}
