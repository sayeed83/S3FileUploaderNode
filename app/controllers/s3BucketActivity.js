var AWS = require('aws-sdk');
var dbConnection = require('../config/database');

const executeSql = async (sql) => {
    // console.log("executeSql :: ", sql);
    return new Promise((resolve, reject) => {
        dbConnection.query(sql, (error, results, fields) => {
            if (error) {
                console.log("sql error :: ", error);
                reject(error);
            }
            else {
                let data = {};
                data.recordset = results;
                console.log('sql success :: ');
                resolve(data);
            }
        });
    });
};

exports.presinedUrl = async (req, res) => {
    let response = {};
    let errors = [];
    let body = req.query;
    console.log(" body ", body);

    try {
        if (!body.fileName) {
            errors.push({
                field: 'fileName',
                description: 'Missing File Name'
            });
        }
        
        if(errors.length > 0) {
            response.status = "error";
            response.message = 'BAD_REQUEST';
            response.data = errors;
            res.status(400).json(response);
        }
        else {

            AWS.config.update({ 
                accessKeyId: process.env.ACCESS_KEY_ID,
                secretAccessKey: process.env.SECRET_ACCESS_KEY,
                region: process.env.REGION,
                signatureVersion: 'v4'
            });
        
            const s3 = new AWS.S3()
            const myBucket = process.env.PRIVAT_BUCKET_NAME;
        
            await s3.createPresignedPost({
              Fields: {
                  // key: uuidv4(),
                key: body.fileName,
              },
              Conditions: [
              //   ["starts-with", "$Content-Type", "image/jpeg"],
                ["content-length-range", 0, 1000000],
              ],
              Expires: 30,
              Bucket: myBucket,
            }, (err, signed) => {
                if(signed) {
                    response.status = "success";
                    console.log(" signed ", signed)
                    response.data = signed;
                    res.status(200).json(response);
                }
                else {
                    console.log(" prsined url error ", err);
                    response.status = "success";
                    response.message = 'BAD_REQUEST';
                    response.data = {};
                    res.status(400).json(response);
                }
                
            });
        }
    }
    catch(err) {
        console.log("catch :: ", err);
        response.status = "error";
        response.message = 'BAD_REQUEST';
        response.data = {};
        res.status(400).json(response);
    }
    
}

exports.reads3Data = async (req, res) => {
    let response = {};
    let errors = [];
    let body = req.query;

    try {
        if (!body.fileName) {
            errors.push({
                field: 'fileName',
                description: 'Missing File Name'
            });
        }
        
        if(errors.length > 0) {
            response.status = "error";
            response.message = 'BAD_REQUEST';
            response.data = errors;
            res.status(400).json(response);
        }
        else {

            AWS.config.update({ 
                accessKeyId: process.env.ACCESS_KEY_ID,
                secretAccessKey: process.env.SECRET_ACCESS_KEY,
                region: process.env.REGION,
                signatureVersion: 'v4'
            });
        
            const s3 = new AWS.S3()
            const myBucket = 'fileuploadsayeed';
            var params = { Bucket: myBucket, Key: body.fileName }; 
            s3.getObject(params, function(err, data) {
                if (err) {

                    response.status = "error";
                    response.message = 'BAD_REQUEST';
                    response.data = err;
                    res.status(400).json(response);
                    // return res.send({ error: err });
                }
                else {
                    // res.send(data.Body);
                    res.send(data.Body);
                }
            });
        }
    }
    catch(err) {
        console.log("catch :: ", err);
        response.status = "error";
        response.message = 'BAD_REQUEST';
        response.data = {};
        res.status(400).json(response);
    }
    
}

exports.reads3Datas = async (req, res) => {
    console.log(" req.params.fileName ", req.params.fileName);
    let fileName = req.params.fileName;
    let response = {};
    let errors = [];
    let body = req.query;

    try {
        if (!fileName) {
            errors.push({
                field: 'fileName',
                description: 'Missing File Name'
            });
        }
        
        if(errors.length > 0) {
            response.status = "error";
            response.message = 'BAD_REQUEST';
            response.data = errors;
            res.status(400).json(response);
        }
        else {

            AWS.config.update({ 
                accessKeyId: process.env.ACCESS_KEY_ID,
                secretAccessKey: process.env.SECRET_ACCESS_KEY,
                region: process.env.REGION,
                signatureVersion: 'v4'
            });
        
            const s3 = new AWS.S3()
            const myBucket = 'fileuploadsayeed';
            var params = { Bucket: myBucket, Key: fileName }; 
            s3.getObject(params, function(err, data) {
                if (err) {

                    response.status = "error";
                    response.message = 'BAD_REQUEST';
                    response.data = err;
                    res.status(400).json(response);
                    // return res.send({ error: err });
                }
                else {
                    // res.send(data.Body);
                    res.send(data.Body);
                }
            });
        }
    }
    catch(err) {
        console.log("catch :: ", err);
        response.status = "error";
        response.message = 'BAD_REQUEST';
        response.data = {};
        res.status(400).json(response);
    }
    
}

exports.addFile = async (req, res) => {
    let response = {};
    let errors = [];
    let body = req.body;
    try {
        if (!body.fileName) {
            errors.push({
                field: 'fileName',
                description: 'Missing File Name'
            });
        }
        
        if(errors.length > 0) {
            response.status = "error";
            response.message = 'BAD_REQUEST';
            response.data = errors;
            res.status(400).json(response);
        }
        else {
            const query = `INSERT INTO s3files (fileName) VALUES ('${body.fileName}')`;
            const result = await executeSql(query);
            if (result.recordset.insertId) {
                const insertedId = result.recordset.insertId;
                const selectFileQuery = `select fileName, createdAt, updatedAt from s3files where id = ${insertedId}`;
                const fileResult = await executeSql(selectFileQuery);
                if (fileResult.recordset.length) {
                    // console.log(fileResult.recordset[0]);
                    response.status = "success";
                    response.data = fileResult.recordset[0];
                    res.status(200).json(response);
                }
                else {
                    response.status = "error";
                    response.message = 'Failed Fetch the inserted data';
                    response.data = {};
                    res.status(400).json(response);
                }
                
            }
            else {
                response.status = "error";
                response.message = 'Failed to insert the data';
                response.data = {};
                res.status(400).json(response);
            }
            
        }
    }
    catch(err) {
        console.log("catch :: ", err);
        response.status = "error";
        response.message = 'BAD_REQUEST';
        response.data = {};
        res.status(400).json(response);
    }
}

exports.getAllFiles = async (req, res) => {
    let response = {};
    let errors = [];
    let body = req.query;
    try {
        const selectFileQuery = `select fileName, createdAt, updatedAt from s3files order by id desc limit 10`;
        const fileResult = await executeSql(selectFileQuery);
        if (fileResult.recordset.length) {
            // console.log(fileResult.recordset[0]);
            response.status = "success";
            response.data = fileResult.recordset;
            res.status(200).json(response);
        }
        else {
            response.status = "success";
            response.message = 'No data found';
            response.data = [];
            res.status(200).json(response);
        }
            
    }
    catch(err) {
        console.log("catch :: ", err);
        response.status = "error";
        response.message = 'BAD_REQUEST';
        response.data = {};
        res.status(400).json(response);
    }
}