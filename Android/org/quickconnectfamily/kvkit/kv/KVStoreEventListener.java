package org.quickconnectfamily.kvkit.kv;

import java.io.Serializable;

public interface KVStoreEventListener {
	
	public void errorHappened(String key, Serializable value, Exception e);
	public boolean shouldStore(String key, Serializable value);
	public void willStore(String key, Serializable value);
	public void didStore(String key, Serializable value);
	
	public boolean shouldDelete(String key);
	public void willDelete(String key);
	public void didDelete(String key);
}
