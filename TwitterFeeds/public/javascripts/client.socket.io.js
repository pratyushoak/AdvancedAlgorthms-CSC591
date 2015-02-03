var server_name = "http://127.0.0.1:3000/";
var server=io.connect(server_name);
console.log("Connecting to server"+server_name);

//var msgElement=document.getElementById('listDisp');
/*
$('#ping').click(function(){
	server.emit('cs-ping',{text:'PING'});
});
*/
var tweetList = $('#listDisp');

server.on('ss-tweets',function(data){
	//if(msgElement!==null)
	//{
//		msgElement.innerHTML=data.text;
//	}
	//server.emit('cs-pong',{text:'PONG'});
 tweetList.prepend('<li>' + data.user+' : '+ data.text + '</li>');
 console.log(data.user+" : "+data.text);
});
/*
server.on('ss-pong',function(data){
	msgElement.innerHTML=data.text;
}*/