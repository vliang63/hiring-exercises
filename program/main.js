var fs = require('fs');
var Converter = require("csvtojson").Converter;
var json2csv = require('json2csv');

fs.readdir('../grouping', function(err, files){
	for(var i = 0; i < files.length; i++){
		if(files[i].split(".")[1] === "csv"){
	    findDups(files[i]);
	  }
	}
});

function findDups(file, matchingIdentifier){
	var fileStream = fs.createReadStream("../grouping/"+file);

	var converter = new Converter({constructResult:true});

	converter.on("end_parsed", function (json) {
	  var holdObj = {};
	  var fields = [];
	    if(json[0].Email){
	      for(var key in json[0]){
	        fields.push(key);
	      }
	      fields.push('Duplicate');
			  for(var i = 0; i < json.length; i++){
	        if(!holdObj[json[i].Email] && holdObj[json[i].Email]!=='undefined'){
	          holdObj[json[i].Email] = [i];
	        }else{
	          if(holdObj[json[i].Email].length===1){
	            json[holdObj[json[i].Email][0]].Duplicate = json[i].Email;
	          }
	          json[i].Duplicate = json[i].Email;
	          holdObj[json[i].Email].push(i);
	        }
	      }
	    }else{
	    	for(var key1 in json[0]){
	    	  fields.push(key1);
	    	}
	    	fields.push('Duplicate');
	    	for(var i = 0; i < json.length; i++){
	        if(!holdObj[json[i].Email1] && holdObj[json[i].Email1]!=='undefined'){
	          holdObj[json[i].Email1] = [i];
	        }else{
	          if(holdObj[json[i].Email1].length===1){
	            json[holdObj[json[i].Email1][0]].Duplicate = json[i].Email1;
	          }
	          json[i].Duplicate = json[i].Email1;
	          holdObj[json[i].Email1].push(i);
	        }

	        if(!holdObj[json[i].Email2] && holdObj[json[i].Email2]!=='undefined'){
	          holdObj[json[i].Email2] = [i];
	        }else{
	          if(holdObj[json[i].Email2].length===1){
	            json[holdObj[json[i].Email2][0]].Duplicate = json[i].Email2;
	          }
	          json[i].Duplicate = json[i].Email2;
	          holdObj[json[i].Email2].push(i);
	        }
	      }
	    }
	  json2csv({ data: json, fields: fields }, function(err, csv) {
	    if (err){
	      console.log('error converting to csv', err);
	    } 
	    console.log(csv);
	  });
	  // console.log(holdObj);
	});

	fileStream.pipe(converter);

};