const sql = require('mysql')
const sqlConfig = {
	host: process.env.DB_HOST,
  	user: process.env.DB_USER,
  	password: process.env.DB_PWD,
	port: process.env.DB_PORT,
	database: process.env.DB_NAME,
	multipleStatements: true
}

module.exports = sqlConfig;

// async function test() {
//     console.log("hello world 2");
// 	try {
// 		// make sure that any items are correctly URL encoded in the connection string
// 		await sql.connect(sqlConfig)
// 		const result = await sql.query`select * from users`
// 		console.dir(result.recordsets);
// 	} catch (err) {
// 		// ... error checks
//         console.dir("err :: ", err);
// 	}
// }
// test();
