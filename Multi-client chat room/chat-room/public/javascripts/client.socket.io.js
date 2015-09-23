// Connect to the local web server at localhost:3000

var server_name = "http://127.0.0.1:3000/";
var server=io.connect(server_name);
console.log("Connecting to server"+server_name);


(jQuery)(function($){

var msgDisp = $('#msgDisp');		//Jquery to find the msgDisp unordered list from layout.jade
var onlineUsersDisp= $('#userDisp');		//Jquery to find the onlineUsersDisp unordered list from layout.jade
var sendButton=$('#send-msg');	//Jquery to find the SEND button
var inputMsg=$('#inputMsg'); //Jquery to find the input message text box
var joinButton=$('#join');	//Jquery to find the join chat button
var introMsg=$('#introduction'); //Jquery to find the introduction message div
var uname_input=$('#uname');	//Jquery to find the username textbox
var chatTable=$('#chatTable'); //Jquery to find the message display table
var user_name;

//initially on connection show only heading, username input box and buttons
server.on('ss-getUserName',function(data)
{
	console.log("\nserver wants user name");
	msgDisp.hide();
	onlineUsersDisp.hide();
	sendButton.hide();
	joinButton.show();
	introMsg.show();
	chatTable.hide();
	inputMsg.hide();
	//chatTable.show();
});

//on clicking join button send value of username textbox to server
joinButton.click(function(){ 			//locate button from webpage
	if($.trim(uname_input.val()) == '') //check if textbox not empty
	{
      alert('Input name can not be left blank.');
   	}
   else
 	{
	server.emit('cs-uname',{text:uname_input.val()});   // on click send to server
	user_name=uname_input.val(); //store user name in variable
	}
});

//on clicking send button send value of message textbox to server
sendButton.click(function(){
	if($.trim(inputMsg.val()) == '')
	{
      alert('Input message can not be left blank.');//check if textbox not empty
   	}
   else
 	{//send to server as json object username:message
	server.emit('cs-newMessage',
		{
		uname:user_name,
		msg:inputMsg.val()
		});   // on click emit PING to server
	}
});
//on receiving message history stored in redis db from server, add the data to the message display table
server.on('ss-msgHistory',function(data){
  $.each(data.msgHistory, function(i, item) {//returned data is list, iterate through it
   		
	msgDisp.prepend( '<li>'+item + '</li>');
   }); 


});

//on receving new message from server add it to message table display
server.on('ss-newMessage',function(data){
	msgDisp.prepend('<li><b>'+data.uname+"</b>:"+data.msg + '</li>');
});

//on successfully joining the chat room display message display table and send text box and button
//hide the username textbox and button
server.on('ss-success',function(data){
	alert(data.text);
	msgDisp.show();
	onlineUsersDisp.show();
	sendButton.show();
	joinButton.hide();
	introMsg.hide();
	chatTable.show();
	inputMsg.show();
	//chatTable.show();

});
//when new user joins chat info received from server, add this info to message display table
server.on('ss-newUser',function(data){
	msgDisp.prepend('<li><b>'+data.text +'</b></li>');
});
//when new user info received update the online users display div
server.on('ss-onlineUsers',function(data){
	onlineUsersDisp.empty();

   $.each(data.onlineUsers, function(i, item) {//returned data is list, iterate through it
	onlineUsersDisp.prepend( '<li><b>'+item + '</b></li>');
   });  // close each()

   
});

});
