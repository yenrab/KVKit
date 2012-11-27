package org.quickconnectfamily.kvkit.kv;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.lang.ref.WeakReference;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import javax.crypto.Cipher;

import org.quickconnectfamily.json.JSONException;
import org.quickconnectfamily.json.JSONUtilities;


import android.app.Activity;
import android.content.Context;


public class PersistanceRunnable implements Runnable {
	private String key;
	private Serializable value;
	private Cipher cipher;
	private int storeCeiling;
	private ConcurrentLinkedQueue<Serializable> valuesByTimeStamp;
	private ConcurrentHashMap<String,Serializable> valuesByKey;
	private KVStoreEventListener listener;
	private WeakReference<Activity> activityRef;
	
	

	public PersistanceRunnable(String key, Serializable value, Cipher cipher,
			int storeCeiling,
			ConcurrentLinkedQueue<Serializable> valuesByTimeStamp,
			ConcurrentHashMap<String, Serializable> valuesByKey,
			int allowedInMemoryElementCount,
			KVStoreEventListener listener,
			WeakReference<Activity> theActivityRef) {
		super();
		this.key = key;
		this.value = value;
		this.cipher = cipher;
		this.storeCeiling = allowedInMemoryElementCount;
		this.valuesByTimeStamp = valuesByTimeStamp;
		this.valuesByKey = valuesByKey;
		this.listener = listener;
		this.activityRef = theActivityRef;
	}



	@Override
	public void run() {
		System.out.println("running");
		String keyToUse = key;
		String jsonStorageString = null;
		/*
		 * Notify the listener and exit early if an error happens
		 */
		try {
			jsonStorageString = JSONUtilities.stringify(value);
			System.out.println("json string: "+jsonStorageString);
		} catch (JSONException e) {
			System.out.println("exception: ");
			e.printStackTrace();
			if(listener != null){
				listener.errorHappened(key, value, new KVStorageException("Bad Value",e));
			}
			return;
		}
		if(cipher != null){
			try {
				keyToUse = new String(cipher.doFinal(keyToUse.getBytes()));
			} catch (Exception e) {
				if(listener != null){
					listener.errorHappened(key, value, new KVStorageException("Bad Key",e));
				}
				return;
			}
			try {
				jsonStorageString = new String(cipher.doFinal(jsonStorageString.getBytes()));
				return;
			} catch (Exception e) {
				if(listener != null){
					listener.errorHappened(key, value, new KVStorageException("Bad Value",e));
				}
				return;
			}
		}
		/*
		 * add the value to the in memory storage if it isn't already there.
		 */
		if(listener != null){
			if(!listener.shouldStore(key, value)){
				return;
			}
			listener.willStore(key, value);
		}
		Serializable existingValue = valuesByKey.get(keyToUse);
		if(existingValue == null){
			valuesByKey.put(keyToUse, value);
			valuesByTimeStamp.add(value);
		}
		
		/*
		 * remove any values above the value count limit defined by the user.
		 */
		while(valuesByTimeStamp.size() > storeCeiling){
			System.out.println("values before remove: "+valuesByTimeStamp);
			Object valueToRemove = valuesByTimeStamp.poll();
			Set<Entry<String, Serializable>> allPairs = valuesByKey.entrySet();
			for(Entry<?, ?> aPair : allPairs){
				if(aPair.getValue().equals(valueToRemove)){
					valuesByKey.remove(aPair.getKey());
					break;
				}
			}
			System.out.println("values after remove: "+valuesByTimeStamp);
		}
		
		/*
		 * Write the value into a file using the key as the file name and the contents being the json string representation.
		 * Lock the file while writing.  Wait for the file to unlock if it is currently locked.
		 */
		
		try {
			KVStore.claimSemaphore(key, Integer.MAX_VALUE);
		} catch (InterruptedException e) {
			listener.errorHappened(key, value, new KVStorageException("Persistance failure",e));
			return;
		}
		try {
			Activity activity = activityRef.get();
			if(activity == null){
				return;
			}
			FileOutputStream persistanceFileOutputStream = activity.openFileOutput(keyToUse, Context.MODE_PRIVATE);
			persistanceFileOutputStream.write(jsonStorageString.getBytes());
			persistanceFileOutputStream.close();
		} catch (FileNotFoundException e) {
			listener.errorHappened(key, value, new KVStorageException("Persistance failure",e));
			KVStore.releaseSemaphore(key, Integer.MAX_VALUE);
			return;
		} catch (IOException e) {
			listener.errorHappened(key, value, new KVStorageException("Persistance failure",e));
			KVStore.releaseSemaphore(key, Integer.MAX_VALUE);
			return;
		}
		KVStore.releaseSemaphore(key, Integer.MAX_VALUE);
		listener.didStore(key, value);
	}

}
