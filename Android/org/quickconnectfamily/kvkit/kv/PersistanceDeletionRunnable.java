package org.quickconnectfamily.kvkit.kv;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;

import android.app.Activity;

public class PersistanceDeletionRunnable implements Runnable {

	Activity theActivity;
	String key;
	String fileName;
	ConcurrentHashMap<String,Semaphore>fileSemaphores;
	ConcurrentLinkedQueue<Serializable> valuesByTimeStamp;
	ConcurrentHashMap<String, Serializable> valuesByKey;
	private KVStoreEventListener listener;
	
	
	public PersistanceDeletionRunnable(Activity theActivity, String key,
			String fileName, ConcurrentHashMap<String, Semaphore> fileSemaphores,
			ConcurrentLinkedQueue<Serializable> valuesByTimeStamp,
			ConcurrentHashMap<String, Serializable> valuesByKey, KVStoreEventListener theListener) {
		super();
		this.theActivity = theActivity;
		this.key = key;
		this.fileName = fileName;
		this.fileSemaphores = fileSemaphores;
		this.valuesByKey = valuesByKey;
		this.valuesByTimeStamp = valuesByTimeStamp;
		this.listener = theListener;
	}

	@Override
	public void run() {
		System.out.println("running deletetion");
		try {
			/*
			 * a file semaphore will exist if the value has been stored previously and not deleted.
			 */
			Semaphore fileSemaphore = fileSemaphores.get(key);
			if(fileSemaphore != null){
				fileSemaphore.acquire(Integer.MAX_VALUE);
				Serializable foundEntity = valuesByKey.get(key);
				listener.willDelete(key);
				//remove it from memory if it is there.
				if(foundEntity != null){
					valuesByKey.remove(key);
					valuesByTimeStamp.remove(foundEntity);
				}
				theActivity.deleteFile(fileName);
				fileSemaphores.remove(fileSemaphore);
				fileSemaphore.release(Integer.MAX_VALUE);
				listener.didDelete(key);
			}
		} catch (Exception e) {
			e.printStackTrace();
			// Do nothing since the value may have been removed by another thread.
		}
	}

}
