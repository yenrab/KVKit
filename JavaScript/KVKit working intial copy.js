var qc = new Object();
qc.ormWorking = false;
qc.multiStoreQueue = null;
qc.storeQueue = [];
//qc.syncQueue = null;
qc.lastSync = null;
qc.complexDataFlag = false;


qc.storeComplexData = function(complexDataFlag){
    if(complexDataFlag){
        qc.complexDataFlag = true;
        /*
        *   Initialize the backing databae
        */
        if(!qc.database){
            qc.database = openDatabase("KVKitDB", "1.0", "a database used to track KVKit ORM entities", 2 * 1024 * 1024);

            qc.database.transaction(function (tx) {  
               tx.executeSql('CREATE TABLE IF NOT EXISTS sync_queue (uuid, modification_time, modification_type)');
            });
        }
    }
}

qc.goORM = function(){
    if(qc.storeQueue.length > 0){
        var aStorageRequest = qc.storeQueue.pop();
        if(aStorageRequest.is_single_request){//single request
            var UUID = aStorageRequest.uuid;
            var type = aStorageRequest.type;
            var aStorable = aStorageRequest.storable;
            console.log('type: '+aStorable.eventType);
            var successNotificationFunction = aStorageRequest.on_success;
            var failureNotificationFunction = aStorageRequest.on_fail;
            
            
            var fieldNamesArray = ['UUID'];
            var fieldValuesArray = [UUID];
            var fieldNames = '(UUID';
            
            var creationFieldNames = '("UUID" TEXT PRIMARY KEY  NOT NULL';
            var fieldValues = "(?";
            var first = true;
            for(var attribute in aStorable){
                if(aStorable[attribute].constructor != Function && attribute != 'UUID'){
                    
                    fieldNames+= ', '+attribute;
                    creationFieldNames += ', '+attribute;
                    fieldValues += ', ?';
                    
                    fieldNamesArray.push(attribute);
                    fieldValuesArray.push((""+aStorable[attribute]));//DON'T do this for attributes that are arrays or other types of Objects!!!!!
                    
                    first = false;
                }
            }
            fieldNames += ')';
            creationFieldNames += ')';
            fieldValues += ')';
            console.log(fieldNamesArray);
            console.log(fieldNames);
            console.log(creationFieldNames);
            console.log(fieldValuesArray);
            
            var getCreationSQL = "SELECT sql FROM sqlite_master WHERE type='table' AND name='"+type+"'";
            //console.log('sync type: '+type);
            
            qc.database.transaction(function (tx) {//transaction execution function 
               tx.executeSql(getCreationSQL,null,function(tx, resultSet) {
               
                                          var insertRecordFunc = function(){
                                                    //insert the record in the table
                                                    var insertSQL = "INSERT OR REPLACE INTO "+type+" "+fieldNames+" VALUES "+fieldValues;
                                                    console.log('inserting using: '+insertSQL+' '+JSON.stringify(fieldValuesArray));
                                                    tx.executeSql(insertSQL, fieldValuesArray);
                                                }
                                          if(resultSet.rows.length == 0){//table doesn't exist
                                                console.log('creating table: '+type);
                                                var tableCreateSQL = 'CREATE TABLE IF NOT EXISTS '+type+' '+creationFieldNames;
                                                tx.executeSql(tableCreateSQL, null, insertRecordFunc);
                                          }
                                          else{//table already exists
                                                console.log('found table: '+type);
                                                if(aStorable.eventType == 'delete'){
                                                    tx.executeSql('DELETE FROM '+type+' WHERE UUID = ?',[UUID], null, function(){aStorable.eventType = 'deleted'});
                                                }
                                                else{
                                                    //inserting or updating a record
                                                    //get the table description to see if the table needs changing.
                                                    var record = resultSet.rows.item(0);
                                                    var creationSQL = record['sql'];
                                                    console.log('creation: '+creationSQL);
                                                    var fieldString = creationSQL.substring(creationSQL.indexOf('(')+1, creationSQL.indexOf(')'));
                                                    //console.log(fieldString);
                                                    var tableFields = fieldString.split(', ');
                                                    
                                                    //console.log(tableFields);
                                                    var fieldsToAdd = new Array();
                                                    var numInstanceFields = fieldNamesArray.length;
                                                    var numTableFields = tableFields.length;
                                                    //clean up the double quotes in the strings
                                                    for(var i = 0; i < numTableFields; i++){
                                                        var tableField = tableFields[i].replace(/\"/g,'');//"//this comment is here to make dashcode color code the rest of the source correnctly.
                                                        tableFields[i] = tableField;
                                                    }
                                                    console.log(' instance fields: '+fieldNamesArray+'  table fields: '+tableFields);
                                                    for(var i = 0; i < numInstanceFields; i++){
                                                        var anInstanceField = fieldNamesArray[i];
                                                        var found = false;
                                                        for(var j = 0; j < numTableFields; j++){
                                                            if(anInstanceField == tableFields[j] || anInstanceField == 'UUID'){
                                                                found = true;
                                                                break;
                                                            }
                                                        }
                                                        if(!found){
                                                            fieldsToAdd.push(anInstanceField);
                                                        }
                                                    }
                                                    var numFieldsToAdd = fieldsToAdd.length;
                                                    if(numFieldsToAdd > 0){
                                                        console.log('altering table.  Adding fields: '+fieldsToAdd);
                                                        for(var i = 0; i < numFieldsToAdd; i++){
                                                            var alterSQL = 'ALTER TABLE "'+type+'" ADD COLUMN "'+fieldsToAdd[i]+'"';
                                                            /*
                                                            * we want the record insertion to happen only after the last
                                                            * table modification so leave the success function null 
                                                            * until the last ALTER TABLE call is made.
                                                            */
                                                            var insertFuncToUse = null;
                                                            if(i == numFieldsToAdd -1){
                                                                insertFuncToUse = insertRecordFunc;
                                                            }
                                                            tx.executeSql(alterSQL, null, insertFuncToUse);
                                                        }
                                                        
                                                    }//end of alter table
                                                    else{
                                                        insertRecordFunc();
                                                    }
                                                }//end of update or insert
                                          }//end of table already exists
                                          });//end of getCreationSQL execution
            }, function(error){failureNotificationFunction(error); qc.goORM(); return true;} 
             , function() {successNotificationFunction(); qc.goORM();});//end of transaction
        }
        else{//batch (transaction) request
        
        }
    }
    else{
        qc.ormWorking = false;
    }
}

qc.SearchTemplate = function(aType){
    this.type = aType;
}

qc.Storable = function(type){
    type = type.toLowerCase();
    this.UUID = qc.generateUUID();
    this.syncType = "sync_type_"+type;
    this.eventType = 'create';
    this.updateTime = new Date().getTime();
    if(qc.multiStoreQueue){
        qc.multiStoreQueue[this.UUID] = this;
    }
    //this is used for syncing.
    //qc.changedStorables.push({syncType:this});
    
    this.setValue = function(aKey, aValue){
        //aKey and aValue must not be null
        if(!aKey || !aValue){
            throw new Error("Both the key: "+aKey+" and the value: "+aValue+" must not be null");
        }
        //aValue must not be an array or a Storable.
        if(this.eventType == 'delete'){
            throw new Error("Attempting to update a deleted Storable");
        }
        this[aKey] = aValue;
        this.eventType = 'update';
        this.updateTime = new Date().getTime();
        if(multiStoreQueue){
            multiStoreQueue[this.UUID] = this;
        }
    }
    this.getValue = function(aKey){
        return this[aKey];
    }
    this.delete = function(){
        this.eventType = 'delete';
        this.updateTime = new Date().getTime();
    }
    this.store = function(failureNotificationFunction, successNotificationFunction){
        if(qc.complexDataFlag){
            if(!qc.multiStoreQueue){
                //true being the first element of the array pushed means that the store request is for a non-multiStorage request.
                qc.storeQueue.push({"is_single_request":true, "storable":this, "uuid":this.UUID, "sync_type":this.syncType,"type":type, "on_fail":failureNotificationFunction, "on_success":successNotificationFunction});
                if(!qc.ormWorking){
                    qc.ormWorking = true;
                    qc.goORM();
                }
            }
            else{
                var value = null;
                if(this.eventType != 'delete'){
                    value = JSON.stringify(this);
                }
                localStorage.setItem(this.UUID, value);
            }
        }
    }
}

qc.startMultiStore = function(){
    qc.multiStoreQueue = {};
}

qc.finalizeMultiStore = function(){
    var sqlQueue = new Array();
    for(var UUID in qc.multiStoreQueue){
        //create a piece of sql for each object found in the queue
        var sql = "";
        //add the sql to the sqlQueue
        sqlQueue.push(sql);
    }
    //create a transaction
}

qc.retrieveStorableById = function(aUUID, type, failureNotificationFunction, successNotificationFunction){
    if(qc.complexDataFlag){
        var tableName = type.toLowerCase();
        qc.database.transaction(function (tx) {//transaction execution function 
               var querySQL = 'SELECT * FROM '+tableName+' WHERE UUID = ?';
               tx.executeSql(querySQL,[aUUID],function(tx, resultSet) {
                    var rows = resultSet.rows;
                        console.log(rows);
                    var numRows = rows.length;
                    var storables = new Array();
                    for(var i = 0; i < numRows; i++){
                        console.log(rows.item(i));
                        var storable = qc.buildInstance(type, rows.item(i));
                        storables.push(storable);
                    }
                    successNotificationFunction(storables);
               });
        }, failureNotificationFunction);
    }
    else{
        var storedString = localStorage.getItem(UUID);
        var aStorable = null;
        if(storedString){
            var anObject = JSON.parse(storedString);
            aStorable = new Storable();
            for(attribute in anObject){
                aStorable[attribute] = anObject[attribute];
            }
        }
        return aStorable;
    }
}
qc.buildInstance = function(type, aRecord){
    var instance = null;
    if(window[type]){//is the type a constructor?
        instance = new window[type]();
    }
    else{
        instance = new qc.Storable(type);
    }
    for(fieldName in aRecord){
        if(fieldName == 'UIID'){
            instance.setUUID(aRecord[fieldName]);
        }
        else{
            instance[fieldName] = aRecord[fieldName];
        }
    }
    return instance;
}

qc.retrieveStorablesLike = function(aTemplate, failureNotificationFunction, successNotificationFunction){
    if(!aTemplate.type){
        throw new Error("Templates used in retrieveStorableLike() must have a type attribute.");
    }
    var whereClause = '';
    var queryParameters = new Array();
    var first = true;
    for(var attribute in aTemplate){
        if(aTemplate[attribute].constructor != Function && attribute != 'type'){
            if(!first){
                whereClause += ' AND ';
            }
            whereClause += attribute + ' = ?';
            queryParameters.push((aTemplate[attribute]+""));
            first = false;
        }
    }
    var querySQL = 'SELECT * FROM '+aTemplate.type.toLowerCase();
    if(whereClause.length > 0){
        querySQL+=' WHERE '+whereClause;
    }
    console.log('sql: '+querySQL);
    var tableName = aTemplate.type.toLowerCase();
        qc.database.transaction(function (tx) {//transaction execution function 
               tx.executeSql(querySQL,queryParameters,function(tx, resultSet) {
                    var rows = resultSet.rows;
                    var numRows = rows.length;
                    var storables = new Array();
                    for(var i = 0; i < numRows; i++){
                        var storable = qc.buildInstance(aTemplate.type, rows.item(i));
                        storables.push(storable);
                    }
                    successNotificationFunction(storables);
               });
        }, failureNotificationFunction);

}

qc.sync = function(){
    if(qc.last_sync == null){
        qc.last_sync = new Date(0).getTime();
    }
    //var serializableObject = {"sync_time":qc.last_sync
    //var JSONString = qc.changedStorables
}


qc.generateUUID = function(){
	var uuid = 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
                                                              var r = Math.random()*16|0, v = c == 'x' ? r : (r&0x3|0x8);
                                                              return v.toString(16);
                                                              });
	return uuid;
}