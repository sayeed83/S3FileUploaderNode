var AWS = require('aws-sdk');
exports.reads3Data = async (req, res) => {
    console.log(' testing ')
    console.log(" req ", req.params.fileName);
    AWS.config.update({ 
      accessKeyId: process.env.ACCESS_KEY_ID,
      secretAccessKey: process.env.SECRET_ACCESS_KEY,
      region: process.env.REGION,
      signatureVersion: 'v4'
    });

    const s3 = new AWS.S3()
    const myBucket = 'fileuploadsayeed';
    const myKey = req.params.fileName;
    var params = { Bucket: myBucket, Key: myKey }; 
    s3.getObject(params, function(err, data) {
      if (err) {
        return res.send({ error: err });
      }
      res.send(data.Body);
    });
}