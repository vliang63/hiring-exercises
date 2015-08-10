var findDups = require("./main.js");
var fs = require("fs");

module.exports = findDupsDirectory = function(directory, matcher){
	fs.readdir(directory, function(err, files){
		for(var i = 0; i < files.length - 1; i++){
			if(files[i].split(".")[1] === "csv"){
		    findDups(directory + "/" + files[i], matcher);
		  }
		}
	});
};
