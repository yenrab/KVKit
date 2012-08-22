/*
 Copyright (c) 2012 Lee Barney
 Permission is hereby granted, free of charge, to any person obtaining a 
 copy of this software and associated documentation files (the "Software"), 
 to deal in the Software without restriction, including without limitation the 
 rights to use, copy, modify, merge, publish, distribute, sublicense, 
 and/or sell copies of the Software, and to permit persons to whom the Software 
 is furnished to do so, subject to the following conditions:
 
 The above copyright notice and this permission notice shall be 
 included in all copies or substantial portions of the Software.
 
 
 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, 
 INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A 
 PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT 
 HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF 
 CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE 
 OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 
 
 */
package org.quickconnectfamily.kvkit.orm;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import android.content.ContentValues;
import android.database.sqlite.SQLiteDatabase;

public class Storable{
	/**
	 * 
	 */
	private String theUUID = null;
	private static HashMap<Class<?>,Boolean> tablesExist = new HashMap<Class<?>,Boolean>();
	
	
	public Storable() {
		this.theUUID = UUID.randomUUID().toString();
	}


	public String getUUID() {
		return theUUID;
	}
	
	protected static void addExistingTable(Class<?>aClass){
		tablesExist.put(aClass, true);
	}


	/*
	 * this call should execute in a separate thread and only store this one Storable.
	 */
	public void store() throws KVKitORMException{
		//facade for the other store method
		KVKitORM.getInstance().store(this);
    }


	public void store(SQLiteDatabase theDb) throws KVKitORMException {
		HashMap<String,Object[]> theAttributes = new HashMap<String,Object[]>();
		boolean needsCommitting = false;
		if(!theDb.inTransaction()){
			theDb.beginTransaction();
			needsCommitting = true;
		}
		/*
		 * Clear out any existing attributes from the tables
		 */
		String[] params = {this.theUUID};
		theDb.delete("storable", "id = ?", params);
		
		Class<?> currentClass = this.getClass();
		while(currentClass != Storable.class){
			collectAttributes(currentClass, theAttributes);
			//System.out.println("attributes: "+theAttributes);
			ContentValues theValues = new ContentValues(theAttributes.size());
			
			
			StringBuilder createTableBuilder = null;
			String className = currentClass.getCanonicalName().replace('.', '_');
			boolean tableExists = (tablesExist.get(currentClass) != null);
			if(!tableExists){
				createTableBuilder = new StringBuilder("CREATE TABLE IF NOT EXISTS ");
				createTableBuilder.append(className);
				createTableBuilder.append(" (");
			}
			
			Set<String> attributeNames = theAttributes.keySet();
			Iterator<String> namesIt = attributeNames.iterator();
			while(namesIt.hasNext()){
				String anAttributeName = namesIt.next();
				Object[] attributeDescription = theAttributes.get(anAttributeName);
				Object value = attributeDescription[1];
				
				if(!tableExists && !anAttributeName.equals("tableExists")){
					//build the type
					createTableBuilder.append('"').append(anAttributeName).append('"');
					if(anAttributeName.equals("theUUID")){
						createTableBuilder.append(" TEXT PRIMARY KEY  NOT NULL  UNIQUE ");
					}
					else{
						//System.out.println("anAttributeName: "+anAttributeName+"class: "+value.getClass().getCanonicalName());
						if(value instanceof Double || value instanceof Float
								|| value instanceof Long || value instanceof Integer
								|| value instanceof Short || value instanceof Boolean
								|| value instanceof Byte){
							createTableBuilder.append(" NUMERIC ");
						}
						else if(value instanceof Storable){
							//dWill add an entry into the relationship table
							createTableBuilder.append(" TEXT ");
						}
						else if( value.getClass().isArray()){
							//Will add entries into the relationship table
							createTableBuilder.append(" TEXT ");
						}
						else if(value instanceof BigInteger || value instanceof byte[] || value instanceof Byte[]){
							createTableBuilder.append(" BLOB ");
						}
						else {
							createTableBuilder.append(" TEXT ");
						}
					}
				}
				
				if(!tableExists && namesIt.hasNext() && !anAttributeName.equals("tableExists")){
					createTableBuilder.append(",");
				}
				if(value instanceof Double){
					theValues.put(anAttributeName, (Double)value);
				}
				if(value instanceof Float){
					theValues.put(anAttributeName, (Float)value);
				}
				else if(value instanceof Long){
					theValues.put(anAttributeName, (Long)value);
				}
				else if(value instanceof Integer){
					theValues.put(anAttributeName, (Integer)value);
				}
				else if(value instanceof Short){
					theValues.put(anAttributeName, (Short)value);
				}
				else if(value instanceof String){
					theValues.put(anAttributeName, (String)value);
				}
				else if(value instanceof Boolean){
					theValues.put(anAttributeName, (Boolean)value);
				}
				else if(value instanceof Byte){
					theValues.put(anAttributeName, (Byte)value);
				}
				else if(value instanceof byte[]){
					theValues.put(anAttributeName, (byte[])value);
				}
				else if(value instanceof Storable){
					//insert a relationship into the relationship table
					String storableKey = UUID.randomUUID().toString();
					ContentValues relationshipValues = new ContentValues(2);
					relationshipValues.put("id", this.theUUID);
					relationshipValues.put("class_name", this.getClass().getCanonicalName());
					theDb.insertWithOnConflict("storable", null, relationshipValues, SQLiteDatabase.CONFLICT_IGNORE);
					
					ContentValues attributeValues = new ContentValues(3);
					attributeValues.put("id", UUID.randomUUID().toString());
					attributeValues.put("storable_fk", storableKey);
					attributeValues.put("attribute_name", anAttributeName);
					attributeValues.put("text_value", ((Storable)value).theUUID);
					theDb.insertWithOnConflict("attribute", null, attributeValues, SQLiteDatabase.CONFLICT_REPLACE);
				}
				//if the value is an array or an arrayList they will need to be handled separately
				else if(value != null && (value.getClass().isArray() || value instanceof ArrayList)){
					String attributeId = UUID.randomUUID().toString();
					if(value instanceof ArrayList){
						ArrayList<?>theValueList = (ArrayList<?>)value;
						for(Object aValue : theValueList){
							ContentValues theInsertionValues = new ContentValues(3);
							theInsertionValues.put("id", UUID.randomUUID().toString());
							theInsertionValues.put("attribute_fk", attributeId);
							addValue(aValue, theInsertionValues);
							theDb.insertWithOnConflict("array", null, theInsertionValues, SQLiteDatabase.CONFLICT_REPLACE);
						}
					}
					else if(value instanceof String[]){
						String[] theStrings = (String[])value;
						for(String aString : theStrings){
							ContentValues theInsertionValues = new ContentValues(3);
							theInsertionValues.put("id", UUID.randomUUID().toString());
							theInsertionValues.put("attribute_fk", attributeId);
							addValue(aString, theInsertionValues);
							theDb.insertWithOnConflict("array", null, theInsertionValues, SQLiteDatabase.CONFLICT_REPLACE);
						}
					}
					else{
						int arrlength = Array.getLength(value);
			            for(int i = 0; i < arrlength; ++i){
							ContentValues theInsertionValues = new ContentValues(3);
							theInsertionValues.put("id", UUID.randomUUID().toString());
							theInsertionValues.put("attribute_fk", attributeId);
			            	addValue(Array.get(value, i), theInsertionValues);
			            	theDb.insertWithOnConflict("array", null, theInsertionValues, SQLiteDatabase.CONFLICT_REPLACE);
			             }
					}
				}
				else if(value != null && value instanceof Map){
					String attributeId = UUID.randomUUID().toString();
					Map<String,Object> valueMap = (Map<String,Object>)value;
					Set<Map.Entry<String, Object>> theEntries = valueMap.entrySet();
					Iterator<Map.Entry<String, Object>> entryIt = theEntries.iterator();
					while(entryIt.hasNext()){
						Map.Entry<String, Object> anEntry = entryIt.next();
						String keyName = anEntry.getKey();
						Object aValue = anEntry.getValue();
						ContentValues theInsertionValues = new ContentValues(3);
						theInsertionValues.put("id", UUID.randomUUID().toString());
						theInsertionValues.put("attribute_fk", attributeId);
						theInsertionValues.put("key_name", keyName);
						addValue(aValue, theInsertionValues);
		            	theDb.insertWithOnConflict("map", null, theInsertionValues, SQLiteDatabase.CONFLICT_REPLACE);
					}
				}
				else if(value != null && value instanceof BigInteger){
					theValues.put(anAttributeName, ((BigInteger)value).toByteArray());
				}
				else if(value != null && value instanceof BigDecimal){
					theValues.put(anAttributeName, ((BigDecimal)value).toEngineeringString());
				}
			}
			if(!tableExists){
				createTableBuilder.append(" )");
			}
			try{
				if(!tableExists){
					String createTableString = createTableBuilder.toString();
					//System.out.println("create: "+createTableString);
					theDb.execSQL(createTableString);
					//System.out.println("done create");
					tablesExist.put(currentClass,true);
				}
				//System.out.println("insert data");
				theDb.insertWithOnConflict(className, null, theValues, SQLiteDatabase.CONFLICT_REPLACE);
				//System.out.println("done insert data");
				if(needsCommitting){
					theDb.setTransactionSuccessful();
				}
			}
			catch (Exception e){
				e.printStackTrace();
				throw new KVKitORMException(e);
			}
			currentClass = currentClass.getSuperclass();
			//System.out.println("currentClass: "+currentClass);
			
		}
		if(needsCommitting){
			theDb.endTransaction();
		}
		
	}


	private ContentValues addValue(Object value, ContentValues arrayValues) throws KVKitORMException {
	
		if(value instanceof Storable){
			Storable theStorable = (Storable)value;
			//use the storable's id as a text value
			arrayValues.put("text_value", theStorable.theUUID);
		}
		else if(value instanceof String){
			arrayValues.put("text_value", ((String)value));
		}
		else if(value instanceof Double || value instanceof Float
				|| value instanceof Long || value instanceof Integer
				|| value instanceof Short || value instanceof Boolean
				|| value instanceof Byte){
				String numberString = String.valueOf(value);
				arrayValues.put("number_value", numberString);
		}
		return arrayValues;
	}
	
	private void collectAttributes(Class<?> aClass, HashMap<String,Object[]> allAttributes) throws KVKitORMException{
		//System.out.println("class name: "+aClass.getCanonicalName());
		Field[] fields = aClass.getDeclaredFields();
		for(int i = 0; i < fields.length; i++){
			try {
				Field aField = fields[i];
				aField.setAccessible(true);
				Object[] description = new Object[2];
				description[0] = aField.getType();
				description[1] = aField.get(this);
				allAttributes.put(aField.getName(), description);
			} catch (Exception e) {
				KVKitORMException kve = new KVKitORMException(e.getLocalizedMessage());
				kve.setStackTrace(e.getStackTrace());
				throw kve;
			}
		}
		return;
	}
	
	private void populateIfNeeded(String attributeName, Object attribute){
		/*
		 * if the attribute is null do the query and set the attribute
		 */
	}
	

}
