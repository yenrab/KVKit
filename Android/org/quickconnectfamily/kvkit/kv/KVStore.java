package org.quickconnectfamily.kvkit.kv;

import java.io.FileInputStream;
import java.io.Serializable;
import java.lang.ref.WeakReference;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.regex.Pattern;

import javax.crypto.Cipher;
import org.quickconnectfamily.json.JSONUtilities;


import android.app.Activity;

public class KVStore {
	private static ConcurrentHashMap<String,Serializable> valuesByKey;
	private static ConcurrentLinkedQueue<Serializable> valuesByTimeStamp;
	private static ConcurrentHashMap<String,Semaphore>fileSemaphores;
	private static Cipher theEncryptCipher;
	private static Cipher theDecryptCipher;
	private static int inMemoryStorageCount;
	private static KVStoreEventListener theListener;
	private static Executor runnableExecutor;
	private static WeakReference<Activity> theActivityRef;
	static{
		valuesByKey = new ConcurrentHashMap<String, Serializable>();
		valuesByTimeStamp = new ConcurrentLinkedQueue<Serializable>();
		fileSemaphores = new ConcurrentHashMap<String,Semaphore>();
		inMemoryStorageCount = 10;
		runnableExecutor = Executors.newCachedThreadPool();
	}
	private KVStore(){
		
	}
	
	public static void claimSemaphore(String semaphoreKey, int claimSize) throws InterruptedException{
		Semaphore fileSemaphore = fileSemaphores.get(semaphoreKey);
		if(fileSemaphore == null){
			fileSemaphore = new Semaphore(Integer.MAX_VALUE, true);
			fileSemaphores.put(semaphoreKey,fileSemaphore);
		}
		fileSemaphore.acquire(claimSize);
	}
	
	public static void releaseSemaphore(String semaphoreKey, int releaseSize){
		Semaphore fileSemaphore = fileSemaphores.get(semaphoreKey);
		if(fileSemaphore != null){
			fileSemaphore.release(releaseSize);
		}
	}
	
	public static void setActivity(Activity anActivity){
		theActivityRef = new WeakReference<Activity>(anActivity);
	}
	
	public static Activity getActivity(){
		return theActivityRef.get();
	}
	
	public static void setStoreEventListener(KVStoreEventListener aListener){
		theListener = aListener;
	}
	public static void setEncryptionCipher(Cipher aCipher){
		theEncryptCipher = aCipher;
	}

	public static void setDecryptionCipher(Cipher aCipher){
		theDecryptCipher = aCipher;
	}
	
	public static void setInMemoryStorageCount(int aCount){
		inMemoryStorageCount = aCount;
	}
	
	public static void storeValue(String key, Serializable value) throws KVStorageException{
		KVStore.storeValue(key, value, null);
	}
	public static void storeValue(String key, Serializable value, Cipher aCipher) throws KVStorageException{
		/*
		 * Make sure the key and value are valid.
		 */
		if(key == null){
			throw new KVStorageException("Missing Key",new NullPointerException("Key can not be null"));
		}
		if(value == null){
			throw new KVStorageException("Missing Value",new NullPointerException("Value can not be null"));
		}
		if(aCipher == null){
			aCipher = theEncryptCipher;
		}
		//start the storage on a separate thread from the pool.
		runnableExecutor.execute(new PersistanceRunnable(key, value, aCipher, inMemoryStorageCount, valuesByTimeStamp, valuesByKey, inMemoryStorageCount, theListener, theActivityRef));
	}
	
	public static Serializable getValue(String key){
		return KVStore.getValue(key, theDecryptCipher);
	}
	
	public static Serializable getValue(String key, Cipher aCipher){
		Serializable existingValue = valuesByKey.get(key);
		/*
		 * if the entity is not already found in the in memory store get it from the file, inflate it, add it to the in memory store, clean up the store if needed,
		 * return the entity.
		 */
		if(existingValue == null){
			String keyToUse = key;
			if(aCipher == null){
				aCipher = theDecryptCipher;
			}
			if(aCipher != null){
				try {
					keyToUse = new String(aCipher.doFinal(keyToUse.getBytes()));
				} catch (Exception e) {
					if(theListener != null){
						theListener.errorHappened(key, null, new KVStorageException("Bad Key",e));
					}
					return null;
				}
			}
			

			existingValue = buildValueFromFile(key, aCipher,
					keyToUse);
		}
		/*
		 * Shift the retrieved value to the front of the queue.
		 */
		valuesByKey.put(key, existingValue);
		valuesByTimeStamp.remove(existingValue);
		valuesByTimeStamp.add(existingValue);
		
		/*
		 * remove any values above the value count limit defined by the user.
		 */
		while(valuesByTimeStamp.size() > inMemoryStorageCount){
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
		return existingValue;
	}

	protected static Serializable buildValueFromFile(String key, Cipher aCipher, String encryptedKey) {
		Serializable aFoundEntity = null;
		
		try {
			KVStore.claimSemaphore(key, 1);
		} catch (InterruptedException e) {
			theListener.errorHappened(key, null, new KVStorageException("Persistance failure",e));
			return null;
		}
		try {
			Activity theActivity = theActivityRef.get();
			if(theActivity == null){
				return null;
			}
			FileInputStream persistanceFileInputStream = theActivity.openFileInput(encryptedKey);
			long size = persistanceFileInputStream.getChannel().size();
			if(size == 0){
				return null;
			}
			byte[] fileBytes = new byte[(int) size];
			persistanceFileInputStream.read(fileBytes);
			persistanceFileInputStream.close();
			if(aCipher != null){
				fileBytes = aCipher.doFinal(fileBytes);
			}
			String textString = new String(fileBytes);
			aFoundEntity = (Serializable)JSONUtilities.parse(textString);
		} catch (Exception e) {
			if(theListener != null){
				theListener.errorHappened(key, null, new KVStorageException("Persistance retrieval error",e));
			}
			KVStore.releaseSemaphore(key, 1);
			return null;
		} 
		KVStore.releaseSemaphore(key, 1);
		return aFoundEntity;
	}
	
	public static void removeValue(String key){
		removeValue(key, null);
	}
	
	public static void removeValue(String key, Cipher aCipher){
		if(key != null && theListener.shouldDelete(key)){
			if(aCipher == null){
				aCipher = theEncryptCipher;
			}
			String keyToUse = key;
			if(aCipher != null){
				try {
					keyToUse = new String(aCipher.doFinal(keyToUse.getBytes()));
				} catch (Exception e) {
					if(theListener != null){
						theListener.errorHappened(key, null, new KVStorageException("Key encryption failure",e));
						return;
					}
				}
			}
			runnableExecutor.execute(new PersistanceDeletionRunnable(theActivityRef, key, keyToUse, fileSemaphores, valuesByTimeStamp, valuesByKey, theListener));
		}
	}
	

	public static Set<Object> getEntities(String keyPath, Object matchValue, boolean inMemoryOnly, Cipher aCipher){
		Set<Object> retValue = new HashSet<Object>();
		String[] keysInPath = null;
		if(keyPath.indexOf(".") == -1){
			keysInPath = new String[1];
			keysInPath[0] = keyPath;
		}
		else{
			keysInPath = keyPath.split(".");
		}
		System.out.println("keys: "+keysInPath);

		ExecutorService searchThreadPool = Executors.newCachedThreadPool();
		Collection<Serializable> topLevelValues = valuesByKey.values();
		Iterator<Serializable> valueIterator = topLevelValues.iterator();
		while(valueIterator.hasNext()){
			searchThreadPool.execute(new SearchRunnable(keysInPath, matchValue, valueIterator.next(), retValue, searchThreadPool,theListener));
		}
		if(!inMemoryOnly){
			if(aCipher == null){
				aCipher = theDecryptCipher;
			}
			Activity theActivity = theActivityRef.get();
			if(theActivity == null){
				return null;
			}
			String[] fileNames = theActivity.fileList();
			for(String aFileName : fileNames){
				searchThreadPool.execute(new LoadAndSearchRunnable(aFileName, aCipher, keysInPath, matchValue, valueIterator.next(), retValue, searchThreadPool,theListener));
			}
		}
		//This line of code is a blocking call that won't complete until all of the spawned threads have completed.
		searchThreadPool.shutdown();
		return retValue;
		
	}
	
	public static Set<Object> getEntities(String keyPath, Object matchValue, boolean inMemoryOnly){
		return getEntities(keyPath, matchValue, inMemoryOnly, null);
	}
	
	public static Set<Object> getEntities(String keyPath, Pattern regex, boolean inMemoryOnly){
		return getEntities(keyPath, (Object)regex, inMemoryOnly, null);
	}

	public static Set<Object> getEntities(String keyPath, Pattern regex, boolean inMemoryOnly, Cipher aCipher){
		return getEntities(keyPath, (Object)regex, inMemoryOnly, aCipher);
	}
}

