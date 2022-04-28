var mysql      = require('mysql');
var connection = mysql.createConnection({
  host     : process.env.DB_SERVER,
  user     : process.env.DB_USER,
  password : process.env.DB_PWD,
  database : process.env.DB_NAME
});

module.exports = connection;