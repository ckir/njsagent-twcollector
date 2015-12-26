setInterval(function() {
	var reply = {
        type: 'process:msg',
        data: {
            time: new Date().toISOString()
        }
    };
    if (typeof process.send !== 'undefined') {
    	process.send(reply);
    } else {
    	console.log(reply);
    }
}, 3000);