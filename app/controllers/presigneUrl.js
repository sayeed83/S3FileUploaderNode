var AWS = require('aws-sdk');
exports.presigneUrl = async (req, res) => {
    console.log(" req.params.fileName ", req.params.fileName);
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
        key: req.params.fileName,
      },
      Conditions: [
      //   ["starts-with", "$Content-Type", "image/jpeg"],
        ["content-length-range", 0, 1000000],
      ],
      Expires: 30,
      Bucket: myBucket,
    }, (err, signed) => {
        console.log(" signed ", signed);
      res.json(signed);
    });
}