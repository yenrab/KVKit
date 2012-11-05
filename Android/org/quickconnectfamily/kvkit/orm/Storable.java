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
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.quickconnectfamily.kvkit.KVKitOnMainThreadException;

import android.content.ContentValues;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.os.Looper;

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
	
	protected final static void addExistingTable(Class<?>aClass){
		tablesExist.put(aClass, true);
	}


	/*
	 * this call should execute in a separate thread and only store this one Storable.
	 */
	public void store() throws KVKitORMException, KVKitClassConfigurationException{
		if (Looper.myLooper() != null && Looper.myLooper() == Looper.getMainLooper()) {
			throw new KVKitOnMainThreadException();
		}
		KVKitORM.getInstance().store(this);
    }


	public void store(SQLiteDatabase theDb) throws KVKitORMException, KVKitClassConfigurationException {
		if (Looper.myLooper() != null && Looper.myLooper() == Looper.getMainLooper()) {
			throw new KVKitOnMainThreadException();
		}

		HashMap<String,Object[]> theAttributes = new HashMap<String,Object[]>();
		Class<?> currentClass = this.getClass();
		while(currentClass != Storable.class){
			theAttributes.clear();
			//System.out.println("storing "+currentClass.getCanonicalName()+" with UUID: "+currentClass.getCanonicalName());
			collectAttributes(currentClass, theAttributes);
			//System.out.println("attributes: "+theAttributes);
			ContentValues theValues = new ContentValues(theAttributes.size());
			

			String className = currentClass.getCanonicalName().replace('.', '_');
			
			StringBuilder createTableBuilder = null;
			boolean tableExists = (tablesExist.get(currentClass) != null);
			//System.out.println(className+" table exists: "+tableExists);
			if(!tableExists){
				createTableBuilder = new StringBuilder("CREATE TABLE IF NOT EXISTS ");
				createTableBuilder.append(className);
				createTableBuilder.append(" (");
			}
			Iterator iterator = theAttributes.keySet().iterator();  
			   //System.out.println("existing attributes");
			while (iterator.hasNext()) {  
			   String key = iterator.next().toString();  
			   String value = theAttributes.get(key).toString();  
			   
			   //System.out.println(key + " " + value);  
			} 
			Set<String> attributeNames = theAttributes.keySet();
			Iterator<String> namesIt = attributeNames.iterator();
			while(namesIt.hasNext()){
				String anAttributeName = namesIt.next();
				//System.out.println("att name: "+anAttributeName);
				Object[] attributeDescription = theAttributes.get(anAttributeName);
				//System.out.println("attrbutes for values: "+Arrays.toString(attributeDescription));
				Object value = attributeDescription[1];
				Field theField = (Field)attributeDescription[2];
				//System.out.println("field: "+theField.getName());
				
				if(!tableExists && !anAttributeName.equals("tablesExist")){
					//build the type
					//System.out.println("working attribute: "+anAttributeName);
					if(anAttributeName.equals("id")){
						//System.out.println("found id");
						createTableBuilder.append("\"id\" TEXT PRIMARY KEY  NOT NULL  UNIQUE ");
					}
					else{
						//System.out.println("anAttributeName: "+anAttributeName+" class: "+theField.getClass().getCanonicalName());
						createTableBuilder.append('"').append(anAttributeName).append('"');
						if(theField.getType().isAssignableFrom(Double.class) 
								|| theField.getType().isAssignableFrom(Float.class)
								|| theField.getType().isAssignableFrom(Long.class) 
								|| theField.getType().isAssignableFrom(Integer.class)
								|| theField.getType().isAssignableFrom(Short.class) 
								|| theField.getType().isAssignableFrom(Boolean.class)
								|| theField.getType().isAssignableFrom(Byte.class)
								|| theField.getType().isAssignableFrom(Character.class)){
							createTableBuilder.append(" NUMERIC ");
						}
						else if(theField.getType().isAssignableFrom(Storable.class)){
							//dWill add an entry into the relationship table
							createTableBuilder.append(" TEXT ");
						}
						else if( theField.getType().isAssignableFrom(Object[].class)){
							//Will add entries into the relationship table
							createTableBuilder.append(" TEXT ");
						}
						else if(theField.getType().isAssignableFrom(BigInteger.class) 
								|| theField.getType().isAssignableFrom(Byte[].class)){
							createTableBuilder.append(" BLOB ");
						}
						else {
							createTableBuilder.append(" TEXT ");
						}
					}
				}
				//System.out.println("done evaluating fields for table creation");
				
				if(!tableExists && namesIt.hasNext() && !anAttributeName.equals("tablesExist")){
					createTableBuilder.append(",");
				}
				//System.out.println("value is: "+value);
				if(value != null){
					//System.out.println("value is type: "+value.getClass()+" is Double? "+(value instanceof Double)+" "+value.toString());
					if(value instanceof Double){
						theValues.put(anAttributeName, (Double)value);
					}
					else if(value instanceof Float){
						//System.out.println("doing float");
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
					else if(value instanceof Date){
						long timeInMillis = ((Date)value).getTime();
						theValues.put(anAttributeName, timeInMillis);
					}
					else if(value instanceof Byte){
						//System.out.println("!!!!!!!!!!!!!!!! byte value "+value);
						theValues.put(anAttributeName, (Byte)value);
					}
					else if(value instanceof byte[]){
						theValues.put(anAttributeName, (byte[])value);
					}
					else if(value instanceof Character){
						theValues.put(anAttributeName, String.valueOf((Character)value));
					}
					else if(value instanceof Storable){
						//((Storable)value).store(theDb);//While this line may save a few lines of code by the programmer it will 
						// cause a large amount of wasted CPU cycles.
						//System.out.println("1 "+value.getClass().getCanonicalName()+" storable has uuid: "+((Storable)value).theUUID);
						
						ContentValues relationshipValues = new ContentValues(2);
						relationshipValues.put("parent_fk", this.theUUID);
						relationshipValues.put("child_fk", ((Storable) value).getUUID());
						relationshipValues.put("attribute_name", anAttributeName);
						relationshipValues.put("attribute_type", value.getClass().getCanonicalName());
						//System.out.println("storing into parent_child: "+relationshipValues.toString());
						theDb.insertWithOnConflict("parent_child", null, relationshipValues, SQLiteDatabase.CONFLICT_REPLACE);
					}
					//if the value is an array or an arrayList they will need to be handled separately
					else if(value.getClass().isArray() || ArrayList.class.isAssignableFrom(value.getClass())){
						//System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@Array or ArrayList@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
						//if(Storable.class.isAssignableFrom(value.getClass())){
							//System.out.println("2 "+value.getClass().getCanonicalName()+" storable has uuid: "+((Storable)value).theUUID);
						//}
						//System.out.println("found an array of "+value.getClass().getCanonicalName());
						/*
						 *
						 * 					Table							Table						Table
						 * 				Parent_Storable_Table			collection_element			parent_child
						 *				id (TEXT) primary key		id (TEXT) primary key		parent_fk (TEXT) primary key
						 *											text_value (TEXT)			child_fk  (TEXT) primary key
						 * 											number_value (NUMBER) 		attribute_name (TEXT) primary key
						 * 											array_order (NUMBER)		attribute_type (TEXT)	
						 * 																		map_key (TEXT)
						 * 											
						 */
						Object[] values = null;

						if(value.getClass().isAssignableFrom(ArrayList.class)){
							//System.out.println("is array list");
							ArrayList<?>theValueList = (ArrayList<?>)value;
							values = theValueList.toArray();
							//System.out.println(Arrays.toString(values));
							//System.out.println("list is now composed of "+values.getClass().getComponentType());
						}
						else if(value.getClass().isArray()){
							//System.out.println("is array");
							values = (Object[])value;
						}
						int arrLength = Array.getLength(values);
						//System.out.println("array len: "+arrLength);
						ContentValues collectionElementInsertionValues = new ContentValues(3);
						ContentValues parentChildInsertionValues = new ContentValues(4);
						for(int i = 0; i < arrLength; ++i){
							collectionElementInsertionValues.clear();
							parentChildInsertionValues.clear();
							Object aValue = Array.get(values, i);
							//System.out.println("storing another "+aValue.getClass());
							collectionElementInsertionValues.put("array_order", i);	
							
							completeContentValuesAndInsertIntoDb(theDb,
									theField, collectionElementInsertionValues,
									parentChildInsertionValues, aValue);
							
						}
					}
					else if(value instanceof Map){
						String attributeId = UUID.randomUUID().toString();
						Map<String,Object> valueMap = (Map<String,Object>)value;
						Set<Map.Entry<String, Object>> theEntries = valueMap.entrySet();
						Iterator<Map.Entry<String, Object>> entryIt = theEntries.iterator();
						ContentValues collectionElementInsertionValues = new ContentValues(3);
						ContentValues parentChildInsertionValues = new ContentValues(5);
						while(entryIt.hasNext()){
							collectionElementInsertionValues.clear();
							parentChildInsertionValues.clear();
							Map.Entry<String, Object> anEntry = entryIt.next();
							String keyName = anEntry.getKey();
							Object aValue = anEntry.getValue();
							parentChildInsertionValues.put("map_key", keyName);
							completeContentValuesAndInsertIntoDb(theDb,
									theField, collectionElementInsertionValues,
									parentChildInsertionValues, aValue);
						}
					}
					else if(value instanceof BigInteger){
						theValues.put(anAttributeName, ((BigInteger)value).toByteArray());
					}
					else if(value instanceof BigDecimal){
						theValues.put(anAttributeName, ((BigDecimal)value).toEngineeringString());
					}
					else{
						//do not throw this exception.  The developer will have objects that should be ignored rather than report them as an error.
						//throw new KVKitClassConfigurationException("ERROR: attempting to store usupported class type: "+value.getClass()
								//+ " found in object of type: "+this.getClass());
					}
				}//end of if value != null
			}
			if(!tableExists){
				createTableBuilder.append(" )");
			}
			try{
				if(!tableExists){
					String createTableString = createTableBuilder.toString();
					//System.out.println("create Table: "+createTableString);
					theDb.execSQL(createTableString);
					//System.out.println("done table create");
					tablesExist.put(currentClass,true);
				}
				//System.out.println("***********inserting into "+className+" data: "+theValues+" *****************");
				long insertResult = theDb.insertWithOnConflict(className, null, theValues, SQLiteDatabase.CONFLICT_REPLACE);
				if(insertResult == -1){
					throw new KVKitORMException("unable to insert "+className+" with values: "+theValues);
				}
				//System.out.println("done insert data "+insertResult);
			}
			catch (Exception e){
				//e.printStackTrace();
				throw new KVKitORMException(e);
			}
			currentClass = currentClass.getSuperclass();
			//System.out.println("currentClass: "+currentClass);
			
		}
	}


	private void completeContentValuesAndInsertIntoDb(SQLiteDatabase theDb,
			Field theField, ContentValues collectionElementInsertionValues,
			ContentValues parentChildInsertionValues, Object aValue) {

		Class attributeType = aValue.getClass();
		String id = UUID.randomUUID().toString();
		parentChildInsertionValues.put("parent_fk", this.theUUID);
		parentChildInsertionValues.put("child_fk", id);
		parentChildInsertionValues.put("attribute_name", theField.getName());
		collectionElementInsertionValues.put("id", id);
		
		parentChildInsertionValues.put("attribute_type", attributeType.getCanonicalName());
		if(Number.class.isAssignableFrom(attributeType)){
			//System.out.println("Inserting a: "+attributeType.getCanonicalName());
			String fieldName = "number_value";
			if(BigDecimal.class.isAssignableFrom(attributeType)
					|| BigInteger.class.isAssignableFrom(attributeType)){
				fieldName = "text_value";
				//System.out.println("Big number class: "+attributeType);
			}
			//System.out.println("assigning number to "+fieldName+" "+((Number)aValue).toString());
			collectionElementInsertionValues.put(fieldName, ((Number)aValue).toString());
		}
		else if(Boolean.class.isAssignableFrom(attributeType)){
			Boolean aBool = (Boolean)aValue;
			//System.out.println("assigning boolean to number_value "+aBool.booleanValue());
			collectionElementInsertionValues.put("number_value", aBool.booleanValue() == true ? 1 : 0);
		}
		else if(Character.class.isAssignableFrom(attributeType)){
			Character aChar = (Character)aValue;
			//System.out.println("char is: "+String.valueOf(aChar.charValue()));
			collectionElementInsertionValues.put("text_value", String.valueOf(aChar.charValue()));
		}
		else if(String.class.isAssignableFrom(attributeType)){
				collectionElementInsertionValues.put("text_value", (String)aValue);	
		}
		else if(Storable.class.isAssignableFrom(attributeType)){
			//System.out.println("Adding: "+attributeType.getCanonicalName()+" storable has uuid: "+((Storable)aValue).theUUID);
			parentChildInsertionValues.put("child_fk", ((Storable)aValue).theUUID);
			collectionElementInsertionValues.put("id", ((Storable)aValue).theUUID);
			collectionElementInsertionValues.put("text_value", ((Storable)aValue).theUUID);	
		}
		else{
			//System.out.println("storing unknown type: "+value.getClass());
			collectionElementInsertionValues.put("text_value", aValue.toString());
			
		}
		//System.out.println("collection element values: "+collectionElementInsertionValues);
		//System.out.println("parent child values: "+parentChildInsertionValues);
		theDb.insertWithOnConflict("collection_element", null, collectionElementInsertionValues, SQLiteDatabase.CONFLICT_REPLACE);
		theDb.insertWithOnConflict("parent_child", null, parentChildInsertionValues, SQLiteDatabase.CONFLICT_REPLACE);
	}
	
	public void remove() throws KVKitORMException{
		if (Looper.myLooper() != null && Looper.myLooper() == Looper.getMainLooper()) {
			throw new KVKitOnMainThreadException();
		}
		KVKitORM.getInstance().remove(this);
	}
	
	public void remove(SQLiteDatabase theDb) throws KVKitORMException{
		if (Looper.myLooper() != null && Looper.myLooper() == Looper.getMainLooper()) {
			throw new KVKitOnMainThreadException();
		}
		//System.out.println(" storable removing");
		Class<?> currentClass = this.getClass();
		Boolean exists = tablesExist.get(currentClass);
		if(exists == null){
			return;//ignore removal of Storables from tables if none of that type have ever been created.
		}
		//delete records from each of the tables in the inheritance.
		while(currentClass != Storable.class){
			String tableName = currentClass.getCanonicalName().replace('.', '_');
			String[] params = {this.getUUID()};
			int numDeleted = theDb.delete(tableName, "id=?", params);
			//System.out.println("deleted "+numDeleted+" records.");
			currentClass = currentClass.getSuperclass();
		}
	}
	
	protected void removeByIds(SQLiteDatabase theDb, String[] idsToRemove){
		//System.out.println(" storable removing by id");
		Class<?> currentClass = this.getClass();
		Boolean exists = tablesExist.get(currentClass);
		if(exists == null){
			return;//ignore removal of Storables from tables if none of that type have ever been created.
		}
		while(currentClass != Storable.class){
			StringBuilder whereBuilder = new StringBuilder();
			for(int i = 0; i < idsToRemove.length; i++){
				if(i != 0){
					whereBuilder.append(" OR ");
				}
				whereBuilder.append("id=?");
			}
			String tableName = currentClass.getCanonicalName().replace('.', '_');
			String[] params = {this.getUUID()};
			int numDeleted = theDb.delete(tableName, whereBuilder.toString(), idsToRemove);
			//System.out.println("deleted "+numDeleted+" records.");
			currentClass = currentClass.getSuperclass();
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
		//System.out.println("collecting attributes for: "+aClass.getCanonicalName());
		/*
		 * get the uuid that is used as the common id across the inheritance structure of the tables
		 */
		for(Class currentClass = aClass; currentClass != null; currentClass = currentClass.getSuperclass()){
			if(currentClass != Storable.class){
				continue;
			}
			try {
				Field uuidField = currentClass.getDeclaredField("theUUID");
				Object[] description = new Object[3];
				description[0] = uuidField.getType();
				description[1] = uuidField.get(this);
				description[2] = uuidField;
				allAttributes.put("id", description);
			} catch (Exception e) {
				throw new KVKitORMException(e);
			}
		}
		Field[] fields = aClass.getDeclaredFields();
		//System.out.println("collecting values for fields: "+Arrays.toString(fields));
		for(int i = 0; i < fields.length; i++){
			try {
				Field aField = fields[i];
				if(aField.getType().isPrimitive() || (aField.getType().isArray() && aField.getType().getComponentType().isPrimitive())){
					throw new KVKitClassConfigurationException("Storables may not have primitive attributes");
				}
				aField.setAccessible(true);
				Object[] description = new Object[3];
				description[0] = aField.getType();
				description[1] = aField.get(this);
				description[2] = aField;
				String fieldName = aField.getName();
				if(fieldName.equals("theUUID")){
					fieldName = "id";
				}
				//System.out.println("the field description is now: "+Arrays.toString(description));
				allAttributes.put(fieldName, description);
			} catch (Exception e) {
				KVKitORMException kve = new KVKitORMException(e.getLocalizedMessage());
				kve.setStackTrace(e.getStackTrace());
				throw kve;
			}
		} 
		return;
	}
	
	private Field findContainingClass( Class aPotentialClass, String attributeName){
		Field resultField = null;
		try{
			resultField = aPotentialClass.getDeclaredField(attributeName);
		}
		catch(NoSuchFieldException e){
			Class parentClass = aPotentialClass.getSuperclass();
			if(parentClass != Storable.class && parentClass != null){
				resultField = findContainingClass(parentClass, attributeName);
			}
		}
		
		return resultField;
	}
	
	protected final void loadIfNeeded(String attributeName) throws KVKitORMException{
		if (Looper.myLooper() != null && Looper.myLooper() == Looper.getMainLooper()) {
			throw new KVKitOnMainThreadException();
		}
		/*
		 * if the attribute is null do the query and set the attribute
		 */
		try {
			//System.out.println("declared fields: "+Arrays.toString(this.getClass().getDeclaredFields()));
			//System.out.println("getting field: "+attributeName);
			
			Field aField = findContainingClass(this.getClass(), attributeName);
			aField.setAccessible(true);
			Object anAttribute = aField.get(this);
			boolean shouldLoad = false;
			if(anAttribute == null){
				shouldLoad = true;
			}
			else if(aField.getType().isArray()){
				load(aField);
			}
			else if(anAttribute instanceof Collection || anAttribute instanceof Map){
				Method sizeMethod = anAttribute.getClass().getDeclaredMethod("size", null);
				Object result = sizeMethod.invoke(anAttribute, null);
				int theSize = ((Integer)result).intValue();
				if(theSize == 0){
					shouldLoad = true;
				}
			}
			if(shouldLoad){
				load(aField);
			}
		} catch (Exception e) {
			throw new KVKitORMException(e);
		}
	}
	
	 protected final void load(String attributeName) throws KVKitORMException {
		if (Looper.myLooper() != null && Looper.myLooper() == Looper.getMainLooper()) {
			throw new KVKitOnMainThreadException();
		}
		/*
		 * force a load regardless of existing data
		 */
		try {
			Field anAttribute = this.getClass().getDeclaredField(attributeName);
			load(anAttribute);
		} catch (Exception e) {
			throw new KVKitORMException(e);
		}
	}
	
	private void load(Field anAttribute) throws KVKitORMException{
		KVKitORM.getInstance().load(this, anAttribute);
	}
	
	protected final void load(SQLiteDatabase theDb, Field anAttribute) throws KVKitORMException{
		if (Looper.myLooper() != null && Looper.myLooper() == Looper.getMainLooper()) {
			throw new KVKitOnMainThreadException();
		}
		//System.out.println(this.getClass().getName()+" loading: "+anAttribute.getType().getName()+" "+anAttribute.getName());
		Class<?> attributeType = anAttribute.getType();
		String attributeName = anAttribute.getName();
		//if it is a Storable
		if(Storable.class.isAssignableFrom(attributeType)){
			String parentStorableTableName = this.getClass().getCanonicalName().replace('.', '_');
			String childStorableTableName = attributeType.getCanonicalName().replace('.', '_');
			/*
			 * 												Table
			 * 											parent_child
			 * 										parent_fk (TEXT) primary key
			 * 										child_fk (TEXT) primary key
			 * 										attribute_name (TEXT)
			 */
			/*
			 * 														Table							
			 * 									Parent_Storable_Table && Child_Storable_Table	(created with the name of the Storable class)		
			 * 												id (TEXT) primary key		
			 * 																			
			 * 																			
			 */
			String sql = "SELECT c.* FROM "
					+parentStorableTableName+" p, "+childStorableTableName
					+" c, parent_child pc WHERE pc.parent_fk = ? AND pc.attribute_name = ? AND p.id = pc.parent_fk AND pc.child_fk = c.id";
			String[] selectionArgs = {this.theUUID, attributeName};
			//System.out.println("sql: "+sql);
			//System.out.println("params: "+Arrays.toString(selectionArgs));
			Cursor theCursor = theDb.rawQuery(sql, selectionArgs);
			//System.out.println("found: "+theCursor.getCount());
			if(theCursor.getCount() > 1){
				throw new KVKitORMException("More than one("+theCursor.getCount()+") record found for the attribute "+attributeName+" for an instance of the class "+parentStorableTableName);
			}
			try {
				if(theCursor.getCount() > 0){
					theCursor.moveToNext();
					Storable aStorable = KVKitORM.getInstance().buildStorableFromRecord(theCursor, (Class<? extends Storable>) attributeType);
					anAttribute.setAccessible(true);
					anAttribute.set(this, aStorable);
				}
				else{
					//System.out.println("found nothing");
				}
			} catch (Exception e) {
				throw new KVKitORMException(e);
			}
		}
		//else if it is map or collection
		else if(AbstractMap.class.isAssignableFrom(attributeType)
				|| Map.class.isAssignableFrom(attributeType)){
			try {
				System.out.println("loading a Map");
				//Array.newInstance(attributeType.getComponentType(), );
				//AbstractCollection resultCollection = (AbstractCollection) attributeType.newInstance();
				
				String parentStorableTableName = this.getClass().getCanonicalName().replace('.', '_');
				String childStorableTableName = attributeType.getCanonicalName().replace('.', '_');
				/*
				 *
				 * 					Table							Table						Table
				 * 				Parent_Storable_Table			collection_element			parent_child
				 *				id (TEXT) primary key		id (TEXT) primary key		parent_fk (TEXT) primary key
				 *											text_value (TEXT)			child_fk  (TEXT) primary key
				 * 											number_value (NUMBER) 		attribute_name (TEXT)
				 * 											array_order (NUMBER)		attribute_type (TEXT)	
				 * 																		map_key (TEXT)
				 * 											
				 */
				
				String sql = "SELECT pc.attribute_type, ce.text_value, ce.number_value, pc.map_key FROM "
						+parentStorableTableName+" p, collection_element ce,"
						+" parent_child pc WHERE pc.parent_fk = ? AND pc.attribute_name = ? AND p.id = pc.parent_fk AND pc.child_fk = ce.id ORDER BY ce.array_order ASC";
				String[] selectionArgs = {this.theUUID, attributeName};
				//System.out.println("SQL: "+sql);
				//System.out.println("selection args: "+Arrays.toString(selectionArgs));
				Cursor theCursor = theDb.rawQuery(sql, selectionArgs);
				//System.out.println("number found: "+theCursor.getCount());
				
				AbstractMap resultMap = null;
				while(theCursor.moveToNext()){
					if(resultMap == null){
						resultMap = (AbstractMap) anAttribute.getType().newInstance();
					}
					buildMulitAttributeElement(theDb,
							anAttribute, attributeName,
							parentStorableTableName, theCursor,
							null, resultMap);
					
					
				}
				anAttribute.setAccessible(true);
				if(resultMap != null){
					//System.out.println("setting array list to: "+resultCollection.toString());
				}
				anAttribute.set(this, resultMap);
			} catch (Exception e) {
				throw new KVKitORMException(e);
			}
			
		}
		else if(AbstractCollection.class.isAssignableFrom(anAttribute.getType()) 
				|| Collection.class.isAssignableFrom(anAttribute.getType())
				|| anAttribute.getType().isArray()){
			try {
				//System.out.println("loading an array or array list");
				//Array.newInstance(attributeType.getComponentType(), );
				//AbstractCollection resultCollection = (AbstractCollection) attributeType.newInstance();
				
				String parentStorableTableName = this.getClass().getCanonicalName().replace('.', '_');
				String childStorableTableName = attributeType.getCanonicalName().replace('.', '_');
				/*
				 *
				 * 					Table							Table						Table
				 * 				Parent_Storable_Table			collection_element			parent_child
				 *				id (TEXT) primary key		id (TEXT) primary key		parent_fk (TEXT) primary key
				 *											text_value (TEXT)			child_fk  (TEXT) primary key
				 * 											number_value (NUMBER) 		attribute_name (TEXT)
				 * 											array_order (NUMBER)		attribute_type (TEXT)	
				 * 																		map_key (TEXT)
				 * 											
				 */
				
				String sql = "SELECT pc.attribute_type, ce.text_value, ce.number_value, ce.array_order FROM "
						+parentStorableTableName+" p, collection_element ce,"
						+" parent_child pc WHERE pc.parent_fk = ? AND pc.attribute_name = ? AND p.id = pc.parent_fk AND pc.child_fk = ce.id ORDER BY ce.array_order ASC";
				String[] selectionArgs = {this.theUUID, attributeName};
				//System.out.println("SQL: "+sql);
				//System.out.println("selection args: "+Arrays.toString(selectionArgs));
				Cursor theCursor = theDb.rawQuery(sql, selectionArgs);
				//System.out.println("number found: "+theCursor.getCount());
				
				
				AbstractCollection resultCollection = null;
				while(theCursor.moveToNext()){
					if(resultCollection == null){
						if(attributeType.isArray()){
							resultCollection = new ArrayList();
						}
						else{
							resultCollection = (AbstractCollection) anAttribute.getType().newInstance();
						}
					}
					buildMulitAttributeElement(theDb,
							anAttribute, attributeName,
							parentStorableTableName, theCursor,
							resultCollection, null);
					
					
				}
				anAttribute.setAccessible(true);
				if(attributeType.isArray() && resultCollection != null){
					Object[] theArray = (Object[])Array.newInstance(attributeType.getComponentType(), resultCollection.size());
					//System.out.println("setting array to :"+Arrays.toString(theArray));
					resultCollection.toArray(theArray);
					anAttribute.set(this, theArray);
				}
				else{
					if(resultCollection != null){
						//System.out.println("setting array list to: "+resultCollection.toString());
					}
					anAttribute.set(this, resultCollection);
				}

				
			} catch (Exception e) {
				throw new KVKitORMException(e);
			}
		}
		//any others
		else{
			throw new KVKitORMException("ERROR: Unsupported attribute type: "+Collection.class.getCanonicalName());
		}
	}


	private void buildMulitAttributeElement(SQLiteDatabase theDb,
			Field anAttribute, String attributeName,
			String parentStorableTableName, Cursor theCursor,
			AbstractCollection resultCollection, AbstractMap resultMap) throws InstantiationException,
			IllegalAccessException, ClassNotFoundException, KVKitORMException,
			NoSuchMethodException, InvocationTargetException {
		
		//System.out.println("found another one");
		//Storable aStorable = KVKitORM.getInstance().buildStorableFromRecord(theCursor, (Class<? extends Storable>) attributeType);
		//build an entity from each record and add it to the collection
		String type = theCursor.getString(0);//the class of the item in the collection
		String textValue = theCursor.getString(1);
		int numberAsInt = theCursor.getInt(2);
		double numberAsDouble  = theCursor.getDouble(2);
		Class instanceType = Class.forName(type);
		//System.out.println("index: "+theCursor.getInt(3)+" found another "+instanceType+" text "+textValue+" number "+numberAsDouble);
		if(Storable.class.isAssignableFrom(instanceType)){
			//System.out.println("adding to results a Storable of type: "+instanceType.getName());
			String aUUID = textValue;
			//load a storable that has aUUID as its' UUID.
			String instanceTableName = instanceType.getCanonicalName().replace('.', '_');
			//System.out.println("all values in table.  Looking for UUID: "+aUUID);
			/*Cursor tempCursor = theDb.rawQuery("SELECT * FROM "+instanceTableName, null);
			while(tempCursor.moveToNext()){
				int numColumns = tempCursor.getColumnCount();
				for(int i = 0; i < numColumns; i++){
					//System.out.print(tempCursor.getString(i)+"\t\t");
				}
			}*/
			//System.out.println("!!!!!******!!!!");
			
			String instanceSql = "SELECT * FROM "
					+instanceTableName+"  WHERE "+instanceTableName+".id = ?";
			String[] instanceSelectionArgs = {aUUID};
			//System.out.println("storable sql: "+instanceSql);
			//System.out.println("args: "+Arrays.toString(instanceSelectionArgs));
			Cursor instanceCursor = theDb.rawQuery(instanceSql, instanceSelectionArgs);
			//System.out.println("found: "+instanceCursor.getCount());
			if(instanceCursor.getCount() > 1){
				throw new KVKitORMException("More than one("+theCursor.getCount()+") record found for the attribute "+attributeName+" for an instance of the class "+parentStorableTableName);
			}
			try {
				if(instanceCursor.getCount() > 0){
					instanceCursor.moveToNext();
					Storable aStorable = KVKitORM.getInstance().buildStorableFromRecord(instanceCursor, (Class<? extends Storable>) instanceType);
					//System.out.println("adding to result collection: "+aStorable);
					if(resultCollection != null){
						resultCollection.add(aStorable);
					}
					else{
						resultMap.put(theCursor.getString(3),aStorable);
					}
				}
			} catch (Exception e) {
				throw new KVKitORMException(e);
			}
		} 
		else if(Number.class.isAssignableFrom(instanceType)){
			//System.out.println("adding some number of type: "+instanceType.getName());
			//String numberAsString = theCursor.getString(2);
			//System.out.println("numVal "+numberValue+" "+numberValue.toString());
			//System.out.println("textVal "+textValue);
			if(Integer.class.isAssignableFrom(instanceType)){
				Constructor theConstructor = instanceType.getConstructor(int.class);
				Object builtValue = theConstructor.newInstance(numberAsInt);
				if(resultCollection != null){
					resultCollection.add(builtValue);
				}
				else{
					resultMap.put(theCursor.getString(3),builtValue);
				}
			}
			else if(Long.class.isAssignableFrom(instanceType)){
				Constructor theConstructor = instanceType.getConstructor(long.class);
				Object builtValue = theConstructor.newInstance(theCursor.getLong(2));
				if(resultCollection != null){
					resultCollection.add(builtValue);
				}
				else{
					resultMap.put(theCursor.getString(3),builtValue);
				}
			}
			else if(Short.class.isAssignableFrom(instanceType)){
				Constructor theConstructor = instanceType.getConstructor(short.class);
				Object builtValue = theConstructor.newInstance(theCursor.getShort(2));
				if(resultCollection != null){
					resultCollection.add(builtValue);
				}
				else{
					resultMap.put(theCursor.getString(3),builtValue);
				}
			}
			else if(Byte.class.isAssignableFrom(instanceType)){
				Constructor theConstructor = instanceType.getConstructor(byte.class);
				Object builtValue = theConstructor.newInstance((byte)numberAsInt);
				if(resultCollection != null){
					resultCollection.add(builtValue);
				}
				else{
					resultMap.put(theCursor.getString(3),builtValue);
				}
			}
			else if(Double.class.isAssignableFrom(instanceType)){
				Constructor theConstructor = instanceType.getConstructor(double.class);
				Object builtValue = theConstructor.newInstance(numberAsDouble);
				if(resultCollection != null){
					resultCollection.add(builtValue);
				}
				else{
					resultMap.put(theCursor.getString(3),builtValue);
				}
			}
			else if(Float.class.isAssignableFrom(instanceType)){
				Constructor theConstructor = instanceType.getConstructor(float.class);
				Object builtValue = theConstructor.newInstance(theCursor.getFloat(2));
				if(resultCollection != null){
					resultCollection.add(builtValue);
				}
				else{
					resultMap.put(theCursor.getString(3),builtValue);
				}
			}
			else if(BigDecimal.class.isAssignableFrom(instanceType)){
				Constructor theConstructor = instanceType.getConstructor(String.class);
				Object builtValue = theConstructor.newInstance(textValue);
				if(resultCollection != null){
					resultCollection.add(builtValue);
				}
				else{
					resultMap.put(theCursor.getString(3),builtValue);
				}
			}
			else if(BigInteger.class.isAssignableFrom(instanceType)){
				Constructor theConstructor = instanceType.getConstructor(String.class);
				Object builtValue = theConstructor.newInstance(textValue);
				if(resultCollection != null){
					resultCollection.add(builtValue);
				}
				else{
					resultMap.put(theCursor.getString(3),builtValue);
				}
			}
			
		}
		else if(String.class.isAssignableFrom(instanceType)){
			//System.out.println("adding a String of type: "+instanceType.getName()+" class "+textValue.getClass()+" value "+textValue);
			Constructor theConstructor = instanceType.getConstructor(String.class);
			Object builtValue = theConstructor.newInstance(textValue);
			if(resultCollection != null){
				resultCollection.add(builtValue);
			}
			else{
				resultMap.put(theCursor.getString(3),builtValue);
			}
		}
		else if(Boolean.class.isAssignableFrom(instanceType)){
			//System.out.println("adding a boolean of type: "+instanceType.getName());
			Constructor theConstructor = instanceType.getConstructor(boolean.class);
			boolean boolValue = false;
			//System.out.println("********************bool int value "+numberValue.intValue()+" *******************");
			if(numberAsInt == 1){
				boolValue = true;
			}
			Object builtValue = theConstructor.newInstance(boolValue);
			if(resultCollection != null){
				resultCollection.add(builtValue);
			}
			else{
				resultMap.put(theCursor.getString(3),builtValue);
			}
		}
		else if(Date.class.isAssignableFrom(instanceType)){
			long timeInMillis = theCursor.getLong(2);
			//System.out.println("adding a date of type: "+instanceType.getName());
			Constructor theConstructor = instanceType.getConstructor(long.class);
			Object builtValue = theConstructor.newInstance(numberAsInt);
			if(resultCollection != null){
				resultCollection.add(builtValue);
			}
			else{
				resultMap.put(theCursor.getString(3),builtValue);
			}
		}
		else if(Character.class.isAssignableFrom(instanceType)){
			//System.out.println("text_value is: "+theCursor.getString(1));
			//System.out.println("adding a character of type: "+instanceType.getName());
			Constructor theConstructor = instanceType.getConstructor(char.class);
			char[] aCharBuffer = new char[2];
			//System.out.println(Arrays.toString(aCharBuffer));
			theCursor.getString(1).getChars(0, 1, aCharBuffer, 0);
			//System.out.println(Arrays.toString(aCharBuffer));
			Object builtValue = theConstructor.newInstance(aCharBuffer[0]);
			if(resultCollection != null){
				resultCollection.add(builtValue);
			}
			else{
				resultMap.put(theCursor.getString(3),builtValue);
			}
		}
		else{
			//System.out.println("inflating unknown type: "+instanceType);
			//Do not throw this exception.  The developer will have attributes of unkown types.  That is OK.
			//throw new KVKitClassConfigurationException("ERROR: inflating unknown type: "+instanceType);
		}
	}


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((theUUID == null) ? 0 : theUUID.hashCode());
		return result;
	}


	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof Storable)) {
			return false;
		}
		Storable other = (Storable) obj;
		if (theUUID == null) {
			if (other.theUUID != null) {
				return false;
			}
		} else if (!theUUID.equals(other.theUUID)) {
			return false;
		}
		return true;
	}
	
	

}
