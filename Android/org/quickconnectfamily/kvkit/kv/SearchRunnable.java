package org.quickconnectfamily.kvkit.kv;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SearchRunnable implements Runnable {
	
	String[] keysInPath;
	Object matchValue;
	Object matchObject;
	Object continueSearchSubValue;
	Set<Object> acceptedObjects;
	Executor searchThreadPool;
	KVStoreEventListener theListener;

	public SearchRunnable(String[] keysInPath, Object matchValue, Object matchObject, Set<Object> acceptedObjects, Executor searchThreadPool, KVStoreEventListener listener) {
		this.keysInPath = keysInPath;
		this.matchValue = matchValue;
		this.matchObject = matchObject;
		this.theListener = listener;
		this.searchThreadPool = searchThreadPool;
		this.acceptedObjects = acceptedObjects;
	}

	private SearchRunnable(String[] keysInPath, Object matchValue,
			Object matchObject, Object continueSearchSubValue, Set<Object> acceptedObjects,
			Executor searchThreadPool, KVStoreEventListener listener) {
		this.keysInPath = keysInPath;
		this.matchValue = matchValue;
		this.continueSearchSubValue = continueSearchSubValue;
		this.matchObject = matchObject;
		this.theListener = listener;
		this.searchThreadPool = searchThreadPool;
		this.acceptedObjects = acceptedObjects;
		
	}

	@Override
	public void run() {
		Object cachedObjectToPotentiallyBeAdded = this.matchObject;
		if(this.continueSearchSubValue != null){
			this.matchObject = this.continueSearchSubValue;
		}
		boolean shouldBeIncluded = shouldInclude();
		if(shouldBeIncluded){
			acceptedObjects.add(cachedObjectToPotentiallyBeAdded);
		}
	}
	

	@SuppressWarnings("unchecked")
	private boolean shouldInclude(){
		String key = keysInPath[0];
		String[] keysTail = new String[keysInPath.length - 1];
		for(int i = 0; i < keysTail.length; i++){
			keysTail[i] = keysInPath[i+1];
		}
		Object theFieldValue = null;
		Field theField = null;
		try {
			theField = matchValue.getClass().getDeclaredField(key);
			theFieldValue = theField.get(matchObject);
			
		} catch (Exception e) {
			theListener.errorHappened(key, null, new KVStorageException("Search key doesn't exist",e));
			return false;
		}
		if(key.indexOf("@") == 0){
			if(key.equals("@toArray")){
				Iterator<Object> fieldValuesIterator = null;
				if(theFieldValue instanceof Map){
					Collection<Object> fieldValues = ((Map<Object,Object>)theFieldValue).values();
					fieldValuesIterator = fieldValues.iterator();
				}
				else if(theFieldValue instanceof List){
					fieldValuesIterator = ((List<Object>)theFieldValue).iterator();
				}
				/*else if it is an array */
				//object arrays
				else if(theFieldValue instanceof Object[]){
					fieldValuesIterator = Arrays.asList(theFieldValue).iterator();
				}
				//Primitive arrays
				else if(theFieldValue.getClass().isArray()){
					Class<?> primitiveArrayClass = theFieldValue.getClass();
					List<Object> convertedArray  = null;
					
					if(int[].class == primitiveArrayClass){
						int[] theArray = (int[])theFieldValue;
						convertedArray = new ArrayList<Object>(theArray.length);
						for(Object aValue : theArray){
							convertedArray.add(aValue);
						}
					}
					else if(short[].class == primitiveArrayClass){
						short[] theArray = (short[])theFieldValue;
						convertedArray = new ArrayList<Object>(theArray.length);
						for(Object aValue : theArray){
							convertedArray.add(aValue);
						}
					}
					else if(long[].class == primitiveArrayClass){
						long[] theArray = (long[])theFieldValue;
						convertedArray = new ArrayList<Object>(theArray.length);
						for(Object aValue : theArray){
							convertedArray.add(aValue);
						}
					}

					else if(double[].class == primitiveArrayClass){
						double[] theArray = (double[])theFieldValue;
						convertedArray = new ArrayList<Object>(theArray.length);
						for(Object aValue : theArray){
							convertedArray.add(aValue);
						}
					}
					else if(float[].class == primitiveArrayClass){
						float[] theArray = (float[])theFieldValue;
						convertedArray = new ArrayList<Object>(theArray.length);
						for(Object aValue : theArray){
							convertedArray.add(aValue);
						}
					}
					else if(char[].class == primitiveArrayClass){

						char[] theArray = (char[])theFieldValue;
						convertedArray = new ArrayList<Object>(theArray.length);
						for(Object aValue : theArray){
							convertedArray.add(aValue);
						}
					}
					else if(byte[].class == primitiveArrayClass){

						byte[] theArray = (byte[])theFieldValue;
						convertedArray = new ArrayList<Object>(theArray.length);
						for(Object aValue : theArray){
							convertedArray.add(aValue);
						}
					}
					fieldValuesIterator = convertedArray.iterator();
				}
				else{
					theListener.errorHappened(key, null, new KVStorageException("Key '"+key+"' is incorrect for an attribute of type '"+theField.getClass()+"'", null));
					return false;
				}

				while(fieldValuesIterator.hasNext()){
					Object aContinueSearchSubValue = fieldValuesIterator.next();
					if(aContinueSearchSubValue instanceof Serializable){

						searchThreadPool.execute(new SearchRunnable(keysTail, matchValue, matchObject, aContinueSearchSubValue, acceptedObjects, searchThreadPool,theListener));
					}
					else{
						
					}
				}
			}
		}
		else if(keysInPath.length == 1){
			
			return isMatch(matchValue, theFieldValue);
		}
		
		return false;
	}

	private boolean isMatch(Object matchValue, Object theFieldValue) {
		if(matchValue.getClass() == Pattern.class){
			//use Match object to match against the object as a string.
			Matcher matcher = 
		        ((Pattern)matchValue).matcher(theFieldValue.toString());
			//uses single find match not subsequent, sequential find match.
			return matcher.find();
			
		}
		else{
			//when not using regular expressions test for equality using the equals method of the value to be matched.
			if(matchValue.equals(theFieldValue)){
				return true;
			}
		}
		return false;
	}
}
