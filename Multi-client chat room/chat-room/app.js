var express = require('express');
var path = require('path');
var favicon = require('serve-favicon');
var logger = require('morgan');
var cookieParser = require('cookie-parser');
var bodyParser = require('body-parser');

var routes = require('./routes/index');
var users = require('./routes/users');

var app = express();
//add redis package 
var redis = require("redis");
//create client
var client = redis.createClient();
/*Set up the server  at localhost to listen on port 3000
*/
var server=require('http').createServer(app);
var port =3000;
var i=0;
var user_count=0;
var user_names={};
var new_user;
/*Create a socket that will be opened when a client connects*/
var sio=require('socket.io').listen(server);
/* Listen on port 3000*/
server.listen(port);
console.log("Socket.io server listening at http://127.0.0.1:" + port);

sio.sockets.on('connection',function(socket){
console.log("Web client connected");

socket.emit("ss-getUserName");
   socket.on('cs-uname',function(data){            //on receiving username
        console.log(data.text);
        user_count++; //increment user count
        //store the new user name appended with count to make it unique
        new_user=data.text+user_count;
        socket.uname=new_user;
        //add the new user to current online users list
        user_names[new_user]=new_user;
        
        var reply_text=new_user+", you have entered the chat room";
            socket.emit('ss-success',{text:reply_text}); //emit success message to client
        //broadcast entry of new user
        reply_text=new_user+" has entered the chat room.";
        socket.broadcast.emit('ss-newUser',{text:reply_text}); //broadcast new user info to everyone
        //retrieve old message data from redis db and emit to new user
        client.lrange('messageHistory', 0, -1, function(err, reply) {
        console.log(reply); 
        socket.emit('ss-msgHistory',{msgHistory:reply});
        });
        //send/update list of users online to everyone
        var onlineUsersList=[];
        for(var key in user_names)
        {
        
            //console.log("user_names:"+user_names[key]);
            onlineUsersList.push(key);
            //console.log("onlineUsersList:"+key);
    
        }
        //emit online users list to everyone
        socket.broadcast.emit('ss-onlineUsers',{
            onlineUsers: onlineUsersList});
         //emit online users list to new joined user
        socket.emit('ss-onlineUsers',{
            onlineUsers: onlineUsersList});

    });

    //if the user enters a new msg, add it to the list in redis and broadcast it to all other users 
    var newMessage; 
    socket.on('cs-newMessage',function(data){
        newMessage = data.uname +":"+data.msg;
        client.rpush('messageHistory',newMessage);
        socket.broadcast.emit('ss-newMessage',
        {
            uname: data.uname,
            msg: data.msg
        });
    //send the new message to the sending client as well
        socket.emit('ss-newMessage',
        {
            uname: data.uname,
            msg: data.msg
        });

        });
        socket.on('disconnect',function(data){
        //on disconnect remake online client list by deleting current user
        var exit_text=socket.uname+" has left the chat room";
        socket.broadcast.emit('ss-disconnectUser',{text:exit_text});
        delete user_names[socket.uname];
        var onlineUsersList=[];
        for(var key in user_names)
        {
        if(user_names.hasOwnProperty(key))
        {
            onlineUsersList.push(key);
        }
        }
        //broadcast new list to all existing users
        socket.broadcast.emit('ss-onlineUsers',{onlineUsers: onlineUsersList});
        //onlineUsers.length = 0;
    });
        
        
    });

// view engine setup
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'jade');

// uncomment after placing your favicon in /public
//app.use(favicon(__dirname + '/public/favicon.ico'));
app.use(logger('dev'));
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));
app.use(cookieParser());
app.use(express.static(path.join(__dirname, 'public')));

app.use('/', routes);
app.use('/users', users);

// catch 404 and forward to error handler
app.use(function(req, res, next) {
    var err = new Error('Not Found');
    err.status = 404;
    next(err);
});

// error handlers

// development error handler
// will print stacktrace
if (app.get('env') === 'development') {
    app.use(function(err, req, res, next) {
        res.status(err.status || 500);
        res.render('error', {
            message: err.message,
            error: err
        });
    });
}

// production error handler
// no stacktraces leaked to user
app.use(function(err, req, res, next) {
    res.status(err.status || 500);
    res.render('error', {
        message: err.message,
        error: {}
    });
});


module.exports = app;
