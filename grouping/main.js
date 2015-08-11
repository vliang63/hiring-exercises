var fs = require("fs");
var Converter = require("csvtojson").Converter;
var json2csv = require("json2csv");

(function(){
	var findDups = function (args){
    //handling the way arguments are passed through in Node from the command line
    var file;
    var matcher;
    if(arguments[0]=== null){
      file = arguments[2];
      matcher = arguments[3];
    }else{
	    file = args[2];
		  matcher = args[3];
    }
    
		var matcherIndex = {same_email: "Email", same_phone: "Phone"};
		matcher = matcherIndex[matcher];
		var matcher1 = matcher + 1;
		var matcher2 = matcher + 2;

    //parameters to convert CSV to JSON
		var fileStream = fs.createReadStream(file);
		var converter = new Converter({constructResult:true});

		converter.on("end_parsed", function (json) {
			if (matcher === "undefined"){
			  return null;
			}
		  var holdObj = {};
		  var fields = [];
		  var checkDups = function(matchingId){
	      for(var j = 0; j < json.length; j++){
          var row = json[j][matchingId];
	        if(!holdObj[row] && row!==""){
	          holdObj[row] = [j];
	        }else if(holdObj[row]){
	          if(holdObj[row].length===1){
	            json[holdObj[row][0]].Duplicate = row;
	          }
	          json[j].Duplicate = json[j][matchingId];
	          holdObj[json[j][matchingId]].push(j);
          }
	      }
      };
      //identify headers for new CSV file
      for(var key in json[0]){
        fields.push(key);
      }
      fields.push("Duplicate");

      //handle duplicate check for different headers
	    if(json[0][matcher]){
			  checkDups(matcher);
	    }else{
        checkDups(matcher1);
        checkDups(matcher2);
      }

		  //convert JSON into CSV and write modified contents to a new file
		  json2csv({data: json, fields: fields}, function(err, csv) {
		    if (err){
		      console.log("error converting to csv", err);
		    }
        var fileNameArray = file.split("/");
		    fs.writeFile("./output/" + "dupsFound-" + matcher + "-" + fileNameArray[fileNameArray.length - 1], csv, function(err){
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