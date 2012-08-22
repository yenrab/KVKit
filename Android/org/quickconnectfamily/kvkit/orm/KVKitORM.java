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

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import android.app.Application;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;

public class KVKitORM {
	private Queue<Storable> storableQueue = null;
	private KVKitOpenHelper theHelper = null;
	private boolean started;
	
	static KVKitORM theKVKit = null;
	
	static{
		theKVKit = new KVKitORM();
	}
	
	public static KVKitORM getInstance(){
		return theKVKit;
	}
	/*
	 * This method should be called only from the Application's onCreate method.
	 */
	public void initialize(Application theApplication, String aName, int aVersion) throws InitializationException{
		if(theHelper == null){
	        /*
	        *   Initialize the backing database
	        */
			theHelper = new KVKitOpenHelper(theApplication, aName,aVersion); 
			
			/*
			 * Get the list of already existent tables.
			 */
			boolean storableTableExists = false;
			SQLiteDatabase theDb = theHelper.getReadableDatabase();
			Cursor tableCursor = theDb.rawQuery("SELECT name FROM sqlite_master WHERE type='table'", null);
			while(tableCursor.moveToNext()){
				String tableName = tableCursor.getString(0);
				//System.out.println("table found: "+tableName);
				if(tableName.equals("storable")){
					storableTableExists = true;
				}
				else if(!tableName.equals("android_metadata") && !tableName.equals("attribute")
						&& !tableName.equals("array") && !tableName.equals("map")){
					tableName = tableName.replace('_', '.');
					try {
						Class<?> aClass = Class.forName(tableName);
						Storable.addExistingTable(aClass);
					} catch (ClassNotFoundException e) {
						throw new InitializationException(e);
					}
				}
			}
			if(!storableTableExists){
				/*
				 * If an attribute can have a numeric XOR a text value. The text value might be an id of another storable if the attribute is a storable.
				 * if an attribute has no number or text value then it must be an array XOR a map. If so there is a one-to-many relationship between the attribute
				 * and the array or map.
				 */
				theDb.execSQL("CREATE TABLE storable(id TEXT PRIMARY KEY  NOT NULL, class_name TEXT )");
				/*
				 * use fkey constraints in this case.  Need to examine triggers to find a speed difference.
				 */
				theDb.execSQL("CREATE TABLE attribute(id TEXT PRIMARY KEY  NOT NULL, storable_fk TEXT REFERENCES storable(id) ON DELETE CASCADE, attribute_name TEXT, number_value NUMBER, text_value TEXT)");
				theDb.execSQL("CREATE TABLE array(id TEXT PRIMARY KEY  NOT NULL, attribute_fk TEXT REFERENCES attribute(id) ON DELETE CASCADE, number_value NUMBER, text_value TEXT)");
				theDb.execSQL("CREATE TABLE map(id TEXT PRIMARY KEY  NOT NULL, attribute_fk TEXT REFERENCES attribute(id) ON DELETE CASCADE, key_name TEXT, number_value NUMBER, text_value TEXT)");
			}
		}
		else{
			throw new InitializationException("KVKit Already Initialized");
		}
	}
	
	private void beginMultiStore(){
		synchronized(this){
			if(!started){
				started = true;
				storableQueue = new ConcurrentLinkedQueue<Storable>();
			}
		}
	}
	
	public void addToMultiStore(Storable aStorable) {
		if(!started){
			beginMultiStore();
		}
		storableQueue.add(aStorable);
		
	}
	
	public void commitMultiStore() throws KVKitORMException {
		if(!started){
			beginMultiStore();
		}
		//do this in a worker thread
		//start a transaction
		SQLiteDatabase theDb = theHelper.getWritableDatabase();
		theDb.setForeignKeyConstraintsEnabled(true);
		theDb.beginTransaction();
		KVKitORMException failedException = null;
		try{
			while(storableQueue.size() > 0){
				Storable aStorable = storableQueue.remove();
				aStorable.store(theDb);
			}
			theDb.setTransactionSuccessful();
		}
		catch(Exception e){
			failedException = new KVKitORMException(e);
		}
		theDb.endTransaction();//does a commit or rollback
		// after the transaction is successful delete the queue.
		storableQueue = null;
		if(failedException != null){
			throw failedException;
		}
	}
	
	
	public void store(Storable aStorable) throws KVKitORMException{
		
		SQLiteDatabase theDb = theHelper.getWritableDatabase();
		KVKitORMException failedException = null;
		theDb.beginTransaction();
		try{
			aStorable.store(theDb);
			theDb.setTransactionSuccessful();
		}
		catch(Exception e){
			failedException = new KVKitORMException(e);
		}
		theDb.endTransaction();//does a commit or rollback
		if(failedException != null){
			throw failedException;
		}
	}
	
	public ArrayList<Storable> get(String keyPath, String value){
		
		return null;
	}
	public ArrayList<Storable> get(Storable anExample, String orderByAttributeName) throws KVKitORMException{
		return get(anExample, null, orderByAttributeName);
	}
	
	public ArrayList<Storable> get(Storable anExample, Field[] fieldsToIgnore, String orderByAttributeName) throws KVKitORMException{
		String tableName = anExample.getClass().getCanonicalName().replace('.', '_');
		SQLiteDatabase theDb = theHelper.getReadableDatabase();
		
		ArrayList<Field>ignoreFields = null;
		if(fieldsToIgnore != null){
			ignoreFields = new ArrayList<Field>(Arrays.asList(fieldsToIgnore));
		}
		if(ignoreFields == null){
			ignoreFields = new ArrayList<Field>();
		}
		try {
			Field hiddenStorableField = Storable.class.getDeclaredField("tablesExist");
			ignoreFields.add(hiddenStorableField);
		} catch (NoSuchFieldException e) {
			e.printStackTrace();
			throw new KVKitORMException(e);
		}
		/*
		 * iterate over all of the fields of the class to assemble the where clause
		 */
		StringBuilder selectionFilterBuilder = null;
		ArrayList<String>selectionArgList = null;
		Field[] theFields = anExample.getClass().getDeclaredFields();
		boolean firstEntry = true;
		for(int i = 0; i < theFields.length; i++){
			Field aField = theFields[i];
			if(!ignoreFields.contains(aField)){
				aField.setAccessible(true);
				try {
					Object aValue = aField.get(anExample);
					System.out.println(aField.getName()+" class: "+aValue.getClass()+" value found: "+aValue);
					if(aValue != null){
						//default values for numeric and boolean fields is ignored.
						if(aValue instanceof Number && ((Number)aValue).doubleValue() == 0.0){
							continue;
						}
						else if(aValue instanceof Boolean 
								&& ((Boolean)aValue).booleanValue() == false){
							continue;
						}
						if(selectionFilterBuilder == null){
							selectionFilterBuilder = new StringBuilder();
							selectionArgList = new ArrayList<String>();
						}
						if(!firstEntry){
							selectionFilterBuilder.append(" AND ");
						}
						else{
							firstEntry = false;
						}
						if(aField.getType().isArray() || aValue instanceof ArrayList){
							if(aValue != null){
								selectionFilterBuilder.append(aField.getName()+" IS NOT NULL");
							}
						}
						else{	
							selectionFilterBuilder.append(aField.getName()+" = ?");
						}
						
						String valueAsString = null;
						if(aValue instanceof Storable){
							valueAsString = ((Storable)aValue).getUUID();
						}
						else if(aValue instanceof String){
							valueAsString = (String)aValue;
						}
						else if(aValue instanceof Number){
							valueAsString = ((Number)aValue).toString();
						}
						else if(aValue instanceof Boolean){
							//valueAsString = ((Boolean)aValue).toString();
							valueAsString = ((Boolean)aValue).compareTo(true) == 1 ? "0": "1";
						}
						//what to do with arrays, arraylists, and maps??????

						selectionArgList.add(valueAsString);
					}
				} catch (Exception e) {
					throw new KVKitORMException(e);
				}
				
			}
		}
		String[] selectionArgs = null;
		if(selectionArgList != null){
			selectionArgs = new String[selectionArgList.size()];
			selectionArgs = selectionArgList.toArray(selectionArgs);
		}
		if(selectionFilterBuilder != null){
			//System.out.println("where: "+selectionFilterBuilder.toString());
			//System.out.println("args: "+selectionArgs);
		}
		//the first null indicates that all field values should be returned. Since grouping doesn't make sense in 
		//this sort of a situation the group by and having parameters are passed null as well.
		String whereString = null;
		if(selectionFilterBuilder != null){
			whereString = selectionFilterBuilder.toString();
		}
		System.out.println("where: "+whereString);
		Cursor theCursor = theDb.query(tableName, null, whereString, selectionArgs, null, null, orderByAttributeName);

		ArrayList<Storable> theFoundStorables = new ArrayList<Storable>();
		Class<? extends Storable> theStorableClass = anExample.getClass();
		
		String[] fieldNames = theCursor.getColumnNames();
		//System.out.println("num records: "+theCursor.getCount());
		try {
			for(int i = 0; i < fieldNames.length; i++){
				theStorableClass.getDeclaredField(fieldNames[i]).setAccessible(true);
			}
			int numColumns = theCursor.getColumnCount();
			while(theCursor.moveToNext()){
				Storable aStorable = theStorableClass.newInstance();
				theFoundStorables.add(aStorable);
				for(int i = 0; i < numColumns; i++){
					int theDBFieldType = theCursor.getType(i);
					String fieldName = theCursor.getColumnName(i);
					Field theField = theStorableClass.getDeclaredField(fieldName);
					theField.setAccessible(true);
					if(theDBFieldType == Cursor.FIELD_TYPE_NULL){
						continue;
					}
					else if(theDBFieldType == Cursor.FIELD_TYPE_STRING){
						theField.set(aStorable, theCursor.getString(i));
					}
					else if(theDBFieldType == Cursor.FIELD_TYPE_INTEGER){
						//System.out.println("Field: "+theField.getName()+" "+theField.getType());
						int fieldValue = theCursor.getInt(i);
						if(boolean.class.isAssignableFrom(theField.getType()) || Boolean.class.isAssignableFrom(theField.getType())){
							boolean fieldValueAsBoolean = fieldValue == 1 ? true : false;
							theField.set(aStorable, fieldValueAsBoolean);
						}
						else if(Integer.class.isAssignableFrom(theField.getType()) || Short.class.isAssignableFrom(theField.getType())){
							theField.set(aStorable, new Integer(fieldValue));
						}
						else{
							theField.set(aStorable, fieldValue);
						}
					}
					else if(theDBFieldType == Cursor.FIELD_TYPE_FLOAT){
						if(Double.class.isAssignableFrom(theField.getType()) || Float.class.isAssignableFrom(theField.getType())){
							theField.set(aStorable, new Double(theCursor.getFloat(i)));
						}
						else if(BigDecimal.class.isAssignableFrom(theField.getType()) ){
							theField.set(aStorable, new BigDecimal(theCursor.getString(i)));
						}
						else{
							theField.set(aStorable, theCursor.getFloat(i));
						}
					}
					else if(theDBFieldType == Cursor.FIELD_TYPE_BLOB){
						if(BigInteger.class.isAssignableFrom(theField.getType())){
							theField.set(aStorable, new BigInteger(theCursor.getBlob(i)));
						}
						else if(theField.getType() == byte[].class ){
							theField.set(aStorable, theCursor.getBlob(i));
						}
					}
				}
			}
		} catch (Exception e) {
			throw new KVKitORMException(e);
		}
		
		return theFoundStorables;
	}
	
	public ArrayList<Object> getUsingKeyPath(String aKeyPath, ArrayList<KVKitComparitor> comparitors, Object compareValue){
		ArrayList<Object> foundObjects = new ArrayList<Object>();
		/*
		 * split the keyPath on '.'
		 * 
		 * Ex. "Stuff.theOthers.otherAges"
		 * Ex. "Stuff.moreOthers.blah_blah"
		 * 
		 * When using the keypath to move down the objects, when an array, ArrayList, or Map is encountered 
		 * use a thread pool & branch the search.  Use this formula to calculate how many threads to pull from
		 * the pool.
		 * 
		 * #items in the collection * (total number of key path items - current item number)/total number of key path items
		 * 
		 * Always join the locally created threads before continuing.
		 * 
		 * 
		 */
		String[] pathElements = aKeyPath.split(".");
		int currentPathCount = 0;
		find(foundObjects, pathElements, pathElements.length,currentPathCount, comparitors);
		return foundObjects;
	}
	
	private void find(ArrayList<Object> foundObjects, String[] pathElements
						, int length, int currentPathCount
						, ArrayList<KVKitComparitor> comparitors){
		
		
		
		if(currentPathCount < length){
			find(foundObjects, pathElements, length, currentPathCount++, comparitors);
		}
		return;
	}
	
	
	public void close(){
		theHelper.close();
	}
}
