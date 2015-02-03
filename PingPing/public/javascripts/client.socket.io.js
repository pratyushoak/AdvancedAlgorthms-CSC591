var server_name = "http://127.0.0.1:4000/";
var server=io.connect(server_name);
//console.log("Connecting to server"+server_name);

var msgElement=document.getElementById('ss-message');

$('#ping').click(function(){
	server.emit('cs-ping',{text:'PING'});
});

server.on('ss-ping',function(data){
	//if(msgElement!==null)
	//{
//		msgElement.innerHTML=data.text;
//	}
	server.emit('cs-pong',{text:'PONG'});
	console.log("Received from server: "+data.text);
});

server.on('ss-pong',function(data){
	msgElement.innerHTML=data.text;
})