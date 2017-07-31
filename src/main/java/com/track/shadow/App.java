package com.track.shadow;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class App {
	public static void main(String[] args) {
		List<SynchronizedService> list = Collections.synchronizedList(new ArrayList<>());
		SynchronizedService service = new SynchronizedService();
		list.add(service);
		service.process();
		
		new Thread(()->{
			while (true) {
				if (list.isEmpty() || list.get(0) == null) {
					SynchronizedService tmp = new SynchronizedService();
					list.add(tmp);
					tmp.process();
				} else if (!list.get(0).isRun()) {
					list.get(0).stop = true;
					list.remove(0);
					SynchronizedService tmp = new SynchronizedService();
					list.add(tmp);
					tmp.process();
				}

				try {
					Thread.sleep(1000);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}).start();
	}
}
