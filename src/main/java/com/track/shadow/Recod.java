package com.track.shadow;

import java.util.HashMap;

public class Recod {
	HashMap<String, Property> map = new HashMap<>();

	public void add(String key, Object value, Class<?> type) {
		map.put(key, new Property(key, value, type));
	}


}