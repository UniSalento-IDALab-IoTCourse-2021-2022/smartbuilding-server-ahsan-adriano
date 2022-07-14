const express = require("express");
const WebSocket = require('ws');
const path = require('path');
var mysql = require('mysql');
const fs = require('fs');
var mqtt=require('mqtt');

const app = express();

const wss = new WebSocket.Server({ port: 3001 });

//var client = mqtt.connect("mqtt://mqtt.eclipseprojects.io",{clientId:"mqttjs012"});
var client = mqtt.connect("mqtt://20.216.178.106:1883");



client.on("connect",function(){
    console.log("connected");
});
client.on("error",function(error){
    console.log("Can't connect"+error);
});








client.on("message",function(topic, message, packet){
    console.log("message is "+ message);
    console.log("topic is "+ topic);
    var messageJSON = JSON.parse(message);

    async function pushInDb(){

         var con = mysql.createConnection({
            host: "mysql-idalab.mysql.database.azure.com",
            user: "idalabsqluser",
            password: "QmluZ28uMzIx",
            port: 3306,
            database : "grafana",
            ssl: {ca: fs.readFileSync("../DigiCertGlobalRootCA.crt.pem")}
        })
             con.connect(function(err) {
            if (err) {
                console.log("!!! Cannot connect to MySQLDB !!! Error:");
                throw err;
            }
            console.log("Connected to MySQLDB!");


            var sql = "CREATE TABLE IF NOT EXISTS sensordth11 (timestamp  TIMESTAMP, sensor VARCHAR(255), temperature DECIMAL (3,1))";
            con.query(sql, function (err, result) {
                if (err) throw err;

            });

            var sql = "CREATE TABLE IF NOT EXISTS electricMeter (timestamp  TIMESTAMP, sensor VARCHAR(255), Consumption DECIMAL (5,4))";
            con.query(sql, function (err, result) {
                if (err) throw err;

            });

            var sql = "CREATE TABLE IF NOT EXISTS solar (timestamp  TIMESTAMP, sensor VARCHAR(255), PannelsPower DECIMAL (5,2), Pannelsefficiency DECIMAL (5,2))";
            con.query(sql, function (err, result) {
                if (err) throw err;

            });

            var sql = "CREATE TABLE IF NOT EXISTS battery (timestamp  TIMESTAMP, sensor VARCHAR(255), BatteryPower DECIMAL (5,1), BatteryCharge DECIMAL (3,1))";
            con.query(sql, function (err, result) {
                if (err) throw err;

            });

        });

        try {

            if(topic=="unisalento/smarthome/raspberry1/sensor/temperature"){
                var temperature = messageJSON.temperature;
                var timestamp = messageJSON.timestamp;
                var sensor = messageJSON.sensor;

                var myDate = timestamp;
                var temp=+temperature
                console.log(myDate);
                console.log(temp);
                var sql = "INSERT INTO sensordth11 (timestamp,sensor,temperature ) VALUES (?,?,?)";
                await con.query(sql, [myDate, sensor ,temp], function (err, result) {
                    if (err) throw err;
                    console.log("1 record inserted");
                });

            }
            if(topic=="unisalento/smarthome/raspberry1/sensorSolarPanel"){
                var pannelsPower=messageJSON.PannelsPower;
                var timestamp = messageJSON.timestamp;
                var sensor =messageJSON.sensor;
                var pannelsEfficency =messageJSON.Pannelsefficiency;

                var myDate =  timestamp;

                var pwr = +pannelsPower;
                var efc = +pannelsEfficency;
                console.log(myDate);
                console.log("power is: ",pwr);
                console.log("efficency is: ",efc);

                var sql = "INSERT INTO solar (timestamp,sensor,PannelsPower,Pannelsefficiency  ) VALUES (?,?,?,?)";
                con.query(sql, [myDate, sensor ,pwr,efc], function (err, result) {
                    if (err) throw err;
                    console.log("1 record inserted");
                });
            }
            if(topic=="unisalento/smarthome/raspberry1/SensorBattery"){

                var BatteryPower = messageJSON.BatteryPower;//"BatteryPower"
                var timestamp = messageJSON.timestamp;
                var sensor = messageJSON.sensor;
                var BatteryCharge=messageJSON.BatteryCharge;
                // create a document to be inserted

                var myDate =  timestamp;
                console.log(myDate)
                var pwr = +BatteryPower;
                var chg = +BatteryCharge;
                console.log("power is: ",pwr)
                console.log("Charge is: ",chg)

                var sql = "INSERT INTO battery (timestamp,sensor,BatteryPower,BatteryCharge) VALUES (?,?,?,?)";
                con.query(sql, [myDate, sensor ,pwr,chg], function (err, result) {
                    if (err) throw err;
                    console.log("1 record inserted");
                });
            }
            if(topic=="unisalento/smarthome/HouseSmartElectricMeter"){

                var Consumption=messageJSON.Consumption;
                var timestamp = messageJSON.timestamp;
                var sensor =messageJSON.sensor;

                var myDate =  timestamp;
                var consumption=+Consumption;
                console.log(myDate);

                var sql = "INSERT INTO electricMeter (timestamp,sensor,Consumption ) VALUES (?,?,?)";
                con.query(sql, [myDate, sensor ,consumption], function (err, result) {
                    if (err) throw err;
                    console.log("1 record inserted");
                });  }
        } finally {
            //await con.end();
        }

    }
    pushInDb().catch(console.dir);
});

var topic="unisalento/smarthome/#";
console.log("subscribing to topic "+topic);
client.subscribe(topic); //single topic