package org.quickconnectfamily.kvkit.kv;

import java.io.Serializable;
import java.util.Set;
import java.util.concurrent.Executor;

import javax.crypto.Cipher;

public class LoadAndSearchRunnable extends SearchRunnable {
	private String aFileName;
	private Cipher aCipher;
	public LoadAndSearchRunnable(String aFileName, Cipher aCipher, String[] keysInPath, Object matchValue,
			Object matchObject, Set<Object> acceptedObjects,
			Executor searchThreadPool, KVStoreEventListener listener) {

		super(keysInPath, matchValue, matchObject, acceptedObjects, searchThreadPool,listener);
		this.aFileName = aFileName;
		this.aCipher = aCipher;
	}

	@Override
	public void run() {
		String key = aFileName;
		if(aCipher != null){
			
			
			try {
				key = new String(aCipher.doFinal(aFileName.getBytes()));
			} catch (Exception e) {
				if(theListener != null){
					theListener.errorHappened(key, null, new KVStorageException("Persistance retrieval error",e));
				}
			}
		}
		Serializable aValue = KVStore.buildValueFromFile(key, aCipher, aFileName);
		if(aValue != null){
			super.run();
		}
		
	}

}
