var fs = require("fs");
var Converter = require("csvtojson").Converter;
var json2csv = require("json2csv");

(function(){
	var findDups = function (args){
	  var file = args[2];
	  var matcher = args[3];
		var matcherIndex = {same_email: "Email", same_phone: "Phone"};
		matcher = matcherIndex[matcher];
	  console.log("matcher", matcher);
		if (matcher === "undefined"){
		  return null;
		}
		var matcher1 = matcher + 1;
		var matcher2 = matcher + 2;
		var fileStream = fs.createReadStream(file);

		var converter = new Converter({constructResult:true});

		converter.on("end_parsed", function (json) {
		  var holdObj = {};
		  var fields = [matcher];
		  var j;
		    if(json[0][matcher]){
		      for(var key in json[0]){
		        fields.push(key);
		      }
		      fields.push("Duplicate");
				  for(j = 0; j < json.length; j++){
		        if(!holdObj[json[j][matcher]] && holdObj[json[j][matcher]]!=="undefined"){
		          holdObj[json[j][matcher]] = [j];
		        }else{
		          if(holdObj[json[j][matcher]].length===1){
		            json[holdObj[json[j][matcher]][0]].Duplicate = json[j][matcher];
		          }
		          json[j].Duplicate = json[j][matcher];
		          holdObj[json[j][matcher]].push(j);
		        }
		      }
		    }else{
		    	for(var key1 in json[0]){
		    	  fields.push(key1);
		    	}
		    	fields.push("Duplicate");
		    	for(j = 0; j < json.length; j++){
		        
		        if(!holdObj[json[j][matcher1]] && holdObj[json[j][matcher1]]!=="undefined"){
		          holdObj[json[j][matcher1]] = [j];
		        }else{
		          if(holdObj[json[j][matcher1]].length===1){
		            json[holdObj[json[j][matcher1]][0]].Duplicate = json[j][matcher1];
		          }
		          json[j].Duplicate = json[j][matcher1];
		          holdObj[json[j][matcher1]].push(j);
		        }
		        
		        if(!holdObj[json[j][matcher2]] && holdObj[json[j][matcher2]]!=="undefined"){
		          holdObj[json[j][matcher2]] = [j];
		        }else{
		          if(holdObj[json[j][matcher2]].length===1){
		            json[holdObj[json[j][matcher2]][0]].Duplicate = json[j][matcher2];
		          }
		          json[j].Duplicate = json[j][matcher2];
		          holdObj[json[j][matcher2]].push(j);
		        }
		      }
		    }
		  //convert back into csv to write modified contents to a new file
		  json2csv({ data: json, fields: fields }, function(err, csv) {
		    if (err){
		      console.log("error converting to csv", err);
		    } 
		    fs.writeFile("../grouping/output/" + file, csv, function(err){
		      if(err){
		        console.log("error writing csv to output", err);
		      }
		    });
		  });

		});

		fileStream.pipe(converter);

	};
  module.exports = findDups;
  findDups(process.argv);
})();