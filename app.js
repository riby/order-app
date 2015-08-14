var express = require('express');
var bodyParser = require('body-parser');
var cassandra = require('cassandra-driver');
var async = require('async');
var path = require('path');

var Long = require('cassandra-driver').types.Long;

var cfenv = require("cfenv");
var appEnv = cfenv.getAppEnv();
var cassandra_ob=appEnv.getService("cassandraOrder");
var amqp = require('amqp');

var authProvider=new cassandra.auth.PlainTextAuthProvider(cassandra_ob.credentials["username"],cassandra_ob.credentials["password"] );

var keyspace_name=cassandra_ob.credentials["keyspace_name"];
//var keyspace_name="test";

var insertOrderInfo='INSERT INTO '+keyspace_name+'.order_info (order_id ,order_status ,order_total ,order_date ,estimated_delivery ,make , model , description , sku , quantity , price_today , carrier , shipping_address1 , shipping_address_city , shipping_address_state , shipping_address_postal_code , billing_address1 , billing_address_city , billing_address_state , billing_address_postal_code )'
+'values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);';
var getOrderById = 'SELECT * FROM '+keyspace_name+'.order_info WHERE order_id = ?;';
var app = express();
var updateOrderInfo='update '+keyspace_name+'.order_info set shipping_address1=?, shipping_address_city=?, shipping_address_state=?, shipping_address_postal_code=? WHERE order_id=?;';

// view engine setup
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'ejs');



app.use(bodyParser.json());

app.use(bodyParser.urlencoded({ extended: false }));




var client = new cassandra.Client( { authProvider:authProvider,contactPoints : cassandra_ob.credentials["node_ips"]} );
client.connect(function(err, result) {
    console.log('Connected.');
});


app.get('/kill', function(req, res) {
    process.exit()
});

app.get('/metadata', function(req, res) {
    res.send(client.hosts.slice(0).map(function (node) {
        return { address : node.address, rack : node.rack, datacenter : node.datacenter }
    }));
});

/**** RabbitMQ Intergration ****/

function rabbitUrl() {
  if (process.env.VCAP_SERVICES) {
    var svcInfo = JSON.parse(process.env.VCAP_SERVICES);
    for (var label in svcInfo) {
      var svcs = svcInfo[label];
      for (var index in svcs) {
        var uri = svcs[index].credentials.uri;
        if (uri.lastIndexOf("amqp", 0) == 0) {
          return uri;
        }
      }
    }
    return null;
  }
  else {
    return "amqp://localhost";
  }
}

var port = process.env.VCAP_APP_PORT || 3000;

function changeOrderStatus(order_id)
{
var updateOrderStatus='update '+keyspace_name+'.order_info set order_status=? WHERE order_id=? ;';
    client.execute(updateOrderStatus,
        ["Authorized", Long.fromString(order_id)],
        function(err,result)
        {
            console.log(err);
          //  if(result!=null)
           // console.log(result);
        });

}
function start()
{

    var queue = conn.queue('order_payment_confirm',{'exclusive':true}, function(q){

      //get all messages for the rabbitExchange
      q.bind('payment.exchange','#');
      console.log("inqueue");
      // Receive messages
      q.subscribe(function (message) {
        // Print messages to stdout
        console.log("received message");
        console.log(message.txId);
        changeOrderStatus(message.txId);
       //  console.log(message.data.toString());
         //res.write( "=" + message.data.toString() + "<br/>");


      });
    });

}

function pushCreatedMessage(order_id)
{

console.log("In push MSG");
    var exchange = conn.exchange('order.exchange', {'type': 'fanout', 'durable': true,'autoDelete': false}, function()
{

    var queue = conn.queue('new_order_queue', {'durable': false, 'exclusive': true},
     function() {
                queue.subscribe(function(msg) {
                //messages.push(order_id+"created"); 
            }); 
            queue.bind(exchange.name, '#');
    });
    
});
exchange.publish('new_order_queue',order_id+ ' created');
    console.log(order_id+" created");

}
console.log("Starting ... AMQP URL: " + 'amqp://60a01b78-ea07-420c-a974-f657ef0dfd6a:spp9peoan7qd020rps9ar4vvom@10.0.16.58/85e7bf72-2f20-4906-9dc8-cad86d8267b2');
var conn = amqp.createConnection({url: 'amqp://60a01b78-ea07-420c-a974-f657ef0dfd6a:spp9peoan7qd020rps9ar4vvom@10.0.16.58/85e7bf72-2f20-4906-9dc8-cad86d8267b2'});
conn.on('ready', start);



/**** Create Table for DB****/
 function createTable(){
    async.series([
        function(next) {
            client.execute('CREATE TABLE IF NOT EXISTS '+keyspace_name+'.order_info (order_id bigint, order_status text, order_total text, order_date text, estimated_delivery text, make text, model text, description text, sku text, quantity text, price_today text, carrier text, shipping_address1 text, shipping_address_city text, shipping_address_state text, shipping_address_postal_code text, billing_address1 text, billing_address_city text, billing_address_state text, billing_address_postal_code text, PRIMARY KEY(order_id));',
                next);
        }],  function(err,result)
        {
            console.log(err);
         //   if(result!=null)
           // console.log(result);
        });
}


app.post('/delete', function(req, res) {
    async.series([
        function(next) {   
                client.execute('drop table '+keyspace_name+'.order_info;',next)

        }],   function(err,result)
        {
            console.log(err);
            if(result!=null)
            console.log(result);
        });
});




app.post('/placeorder', function(req, res) {
  
    client.execute(insertOrderInfo,
        [Long.fromString(req.body.order_id),req.body.order_status,req.body.order_total,req.body.order_date,req.body.estimated_delivery, 
        req.body.make, req.body.model, req.body.description, req.body.sku, req.body.quantity,
         req.body.price_today, req.body.carrier, req.body.shipping_address1, req.body.shipping_address_city, req.body.shipping_address_state, 
         req.body.shipping_address_postal_code, req.body.billing_address1, req.body.billing_address_city, req.body.billing_address_state, req.body.billing_address_postal_code],
        function(err,result)
        {
            console.log(err);
           // if(result!=null)
            //console.log(result);
        });
    pushCreatedMessage(req.body.order_id);
        res.send("Order Inserted "+req.body.order_id);
});



 app.get('/getorder/:id', function(req, res) {
    client.execute(getOrderById, [ Long.fromString(req.params.id) ], function(err, result) {
        if (err) {
            res.status(404).send({ msg : 'order_info not found.' });
        } else {
        res.send(result.rows.map(function (node) {
       
        return { order_id:node.order_id,  order_status: node.order_status, order_total: node.order_total,  order_date: node.order_date,estimated_delivery : node.estimated_delivery, 
         make: node.make,  model: node.model,  description: node.description, sku : node.sku,  quantity: node.quantity,
          price_today: node.price_today,  carrier: node.carrier,  shipping_address1: node.shipping_address1,  shipping_address_city: node.shipping_address_city,  shipping_address_state: node.shipping_address_state, 
shipping_address_postal_code : node.shipping_address_postal_code,billing_address1 : node.billing_address1,  billing_address_city: node.billing_address_city,  billing_address_state: node.billing_address_state,billing_address_postal_code : node.billing_address_postal_code

        }
    
    }));
              
             }
    });
});

app.get('/getallorders', function(req, res) {
  var getAllOrders = 'SELECT * FROM '+keyspace_name+'.order_info';
    client.execute(getAllOrders, function(err, result) {
        if (err) {
            res.status(404).send({ msg : 'order_info not found.' });
        } else {
        res.send(result.rows.map(function (node) {
       
        return { order_id:node.order_id,  order_status: node.order_status, order_total: node.order_total,  order_date: node.order_date,estimated_delivery : node.estimated_delivery, 
         make: node.make,  model: node.model,  description: node.description, sku : node.sku,  quantity: node.quantity,
          price_today: node.price_today,  carrier: node.carrier,  shipping_address1: node.shipping_address1,  shipping_address_city: node.shipping_address_city,  shipping_address_state: node.shipping_address_state, 
shipping_address_postal_code : node.shipping_address_postal_code,billing_address1 : node.billing_address1,  billing_address_city: node.billing_address_city,  billing_address_state: node.billing_address_state,billing_address_postal_code : node.billing_address_postal_code

        }
    
    }));
              
             }
    });
});


//Code Block change
/*
app.get('/getallorderswith/:status', function(req, res) {
  var getAllOrders = 'SELECT * FROM '+keyspace_name+'.order_info';
  var st=req.params.status;
    client.execute(getAllOrders, function(err, result) {
        if (err) {
            res.status(404).send({ msg : 'Similar orders not found.' });
        } else {
        res.send(result.rows.map(function (node) {
          console.log(node.order_status+"   diff  "+st);
          if(node.order_status==st) 
        return { order_id:node.order_id,  order_status: node.order_status, order_total: node.order_total,  order_date: node.order_date
        }
       
    
    }));
              
             }
    });
});
*/

app.all("/", function (req, res, next) {
    res.header("Access-Control-Allow-Origin", "*");
    res.header("Access-Control-Allow-Headers", "Cache-Control, Pragma, Origin, Authorization, Content-Type, X-Requested-With");
    res.header("Access-Control-Allow-Methods", "GET, PUT, POST");
    return next();
});

app.post('/order/change', function(req, res) {
var updateOrderStatus='update '+keyspace_name+'.order_info set order_status=? WHERE order_id=? ;';
    client.execute(updateOrderStatus,
        [req.body.order_status, Long.fromString(req.body.order_id)],
        function(err,result)
        {
            console.log(err);
          //  if(result!=null)
           // console.log(result);
        });
    res.send("Order status changed to "+req.body.order_status);
      
});


var server = app.listen( appEnv.port || 3000, function() {
    createTable();
    console.log('Listening on port %d', server.address().port);
});
