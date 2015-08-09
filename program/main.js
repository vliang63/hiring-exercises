var fs = require('fs');
var Converter = require("csvtojson").Converter;
var json2csv = require('json2csv');

fs.readdir('../grouping', function(err, files){
	for(var i = 0; i < files.length - 1; i++){
		if(files[i].split(".")[1] === "csv"){
	    findDups(files[i], "same_phone");
	  }
	}
});

function findDups(file, matcher){
  matcher = matcher === "same_email" ? "Email" : "Phone";
  var matcher1 = matcher + 1;
  var matcher2 = matcher + 2;
	var fileStream = fs.createReadStream("../grouping/" + file);

	var converter = new Converter({constructResult:true});

	converter.on("end_parsed", function (json) {
	  var holdObj = {};
	  var fields = [];
	    if(json[0][matcher]){
	      for(var key in json[0]){
	        fields.push(key);
	      }
	      fields.push('Duplicate');
			  for(var i = 0; i < json.length; i++){
	        if(!holdObj[json[i][matcher]] && holdObj[json[i][matcher]]!=='undefined'){
	          holdObj[json[i][matcher]] = [i];
	        }else{
	          if(holdObj[json[i][matcher]].length===1){
	            json[holdObj[json[i][matcher]][0]].Duplicate = json[i][matcher];
	          }
	          json[i].Duplicate = json[i][matcher];
	          holdObj[json[i][matcher]].push(i);
	        }
	      }
	    }else{
	    	for(var key1 in json[0]){
	    	  fields.push(key1);
	    	}
	    	fields.push('Duplicate');
	    	for(var i = 0; i < json.length; i++){
	        
	        if(!holdObj[json[i][matcher1]] && holdObj[json[i][matcher1]]!=='undefined'){
	          holdObj[json[i][matcher1]] = [i];
	        }else{
	          if(holdObj[json[i][matcher1]].length===1){
	            json[holdObj[json[i][matcher1]][0]].Duplicate = json[i][matcher1];
	          }
	          json[i].Duplicate = json[i][matcher1];
	          holdObj[json[i][matcher1]].push(i);
	        }
	        
	        if(!holdObj[json[i][matcher2]] && holdObj[json[i][matcher2]]!=='undefined'){
	          holdObj[json[i][matcher2]] = [i];
	        }else{
	          if(holdObj[json[i][matcher2]].length===1){
	            json[holdObj[json[i][matcher2]][0]].Duplicate = json[i][matcher2];
	          }
	          json[i].Duplicate = json[i][matcher2];
	          holdObj[json[i][matcher2]].push(i);
	        }
	      }
	    }
	  json2csv({ data: json, fields: fields }, function(err, csv) {
	    if (err){
	      console.log('error converting to csv', err);
	    } 
      //overwrite and replace old csv file with new one here
      fs.writeFile('../grouping/output/' + file, csv, function(err){
        if(err){
	        console.log('error writing csv to output', err);
        }
      });
	  });

	});

	fileStream.pipe(converter);

};