var AWS = require('aws-sdk');
exports.reads3Data = async (req, res) => {
    console.log(" req ", req.params.fileName);
    AWS.config.update({ 
        accessKeyId: 'AKIA4BQMUAZRZ742TBLR',
        secretAccessKey: '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',
        region: 'ap-south-1',
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