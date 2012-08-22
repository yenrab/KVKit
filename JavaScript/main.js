var people = new Array();
function load(){
    //clear out the old test data
    var database = openDatabase("KVKitDB", "1.0", "a database used to track KVKit ORM entities", 2 * 1024 * 1024);
    database.transaction(function (tx) {  
       tx.executeSql('DROP TABLE person');
    });
    
    
    
    //use the ORM instead of KeyValue storage
    qc.storeComplexData(true);
}

function Person(){
    qc.Storable.call(this, 'Person');
}

function addPerson(){
    var aPersonToAdd = new Person();
    aPersonToAdd.age = 8;
    aPersonToAdd.name = 'bob'
    aPersonToAdd.stuff = ['friend', 7, 3.2, new Date()];
    aPersonToAdd.store(failed, function(){updateDisplay(aPersonToAdd);succeeded()});
    people.push(aPersonToAdd);
}

function getPerson(){
    var personUUID = document.getElementById('uuidInput').value;
    qc.retrieveStorableById(personUUID, 'Person', failed, foundPersonResult);
}

function getByTemplate(){
    var attName = document.getElementById('attNameInput').value;
    var attValue = document.getElementById('attValueInput').value;
    
    var template = new qc.SearchTemplate('Person');
    template[attName] = attValue;
    qc.retrieveStorablesLike(template, failed, foundPeople);
}

function foundPeople(aResult){
    document.getElementById('display').innerText = '';
    if(aResult){
        var numResults = aResult.length;
        for(var i = 0; i < numResults; i++){
            var personJSON = JSON.stringify(aResult[i]);
            console.log(personJSON);
            displayString = 'JSON: '+personJSON;
            document.getElementById('display').innerText += '\n'+displayString;
        }
    }

}

function foundPersonResult(aResult){
    updateDisplay(aResult);
    succeeded();
}

function modifyPerson(){
    var attName = document.getElementById('attNameInput').value;
    var attValue = document.getElementById('attValueInput').value;
    var personUUID = document.getElementById('uuidInput').value;
    var numPeople = people.length;
    updateDisplay(null);
    for(var i = 0; i < numPeople; i++){
        var aPerson = people[i];
        if(aPerson.UUID == personUUID){
            aPerson[attName] = attValue;
            aPerson.store(failed, function(){updateDisplay(aPerson);succeeded()});
            break;
        }
    }
}

function deletePerson(){
    var personUUID = document.getElementById('uuidInput').value;
    var numPeople = people.length;
    updateDisplay(null);
    for(var i = 0; i < numPeople; i++){
        var aPerson = people[i];
        if(aPerson.UUID == personUUID){
            aPerson.delete();
            aPerson.store(failed, function(){updateDisplay(aPerson);succeeded()});
            break;
        }
    }
}

function updateDisplay(aResult){
    var displayString = "";
    if(aResult && aResult.constructor == Array){
        aResult = aResult[0];
    }
    if(aResult){
        var personJSON = JSON.stringify(aResult);
        console.log(personJSON);
        displayString = 'JSON: '+personJSON;
    }
    document.getElementById('display').innerText = displayString;
    
}



function succeeded(){
    console.log('woot!');

}

function failed(error){

    console.log('oops '+JSON.stringify(error));
}