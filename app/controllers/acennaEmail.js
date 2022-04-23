var aws = require('aws-sdk');
var nodemailer = require("nodemailer");
aws.config.update({
    accessKeyId: process.env.accessKeyId,
    secretAccessKey: process.env.secretAccessKey,
});

// create Nodemailer SES transporter
let transporter = nodemailer.createTransport({
    host: process.env.EMAIL_HOST,
    port: process.env.EMAIL_PORT,
    secure: true, // true for 465, false for other ports
    auth: {
      user: process.env.EMAIL_USER, // generated ethereal user
      pass: process.env.EMAIL_PASSWORD, // generated ethereal password
    }
});

module.exports = async function(event) {
    // console.log(" Param", event);

    try
    {
        let mailOptions = {};
        // prepare required fields to send email
        mailOptions = {
            from: event.from,
            subject: event.subject,
            html: event.html,
            to: event.to
        };
        /* block to add cc email address if received to email params(mailOptions) */
        if (event.emailCc) {
            // mailOptions.cc = event.emailCc;
        }
        /* block to add attachments if received to email params(mailOptions) */
        if (event.emailAttachment) {
            mailOptions.attachments = event.emailAttachment;    // emailAttachment must be array of object
        }
        
        // function call to send email
        let responseFromMail = await sendMail(mailOptions);
        /* block to return response depending on sendMail() status  */
        if(responseFromMail === 1)
        {
            return {
                statusCode: 200,
                headers: {
                    "Access-Control-Allow-Origin": "*"
                },
                body: JSON.stringify({ status: 'success', message: 'Email sent' }),
            };
            callback(null, {
                statusCode: 200,
                headers: {
                    "Access-Control-Allow-Origin": "*"
                },
                body: JSON.stringify({ status: 'success', message: 'Email sent' }),
            });
        }
        else
        {
            return {
                    statusCode: 400,
                    headers: {
                        "Access-Control-Allow-Origin": "*"
                    },
                    body: JSON.stringify({ status: 'error', message: 'Error in sending email' }),
                };
            // callback(null, {
            //     statusCode: 400,
            //     headers: {
            //         "Access-Control-Allow-Origin": "*"
            //     },
            //     body: JSON.stringify({ status: 'error', message: 'Error in sending email' }),
            // });
        }
    }
    catch (error)
    {
        return {
            statusCode: 400,
            headers: {
                "Access-Control-Allow-Origin": "*"
            },
            body: JSON.stringify({ status: 'error', message: 'Error in sending mail', error: error }),
        };
        // callback(null, {
        //     statusCode: 400,
        //     headers: {
        //         "Access-Control-Allow-Origin": "*"
        //     },
        //     body: JSON.stringify({ status: 'error', message: 'Error in sending mail', error: error }),
        // });
    }
}


/* Begin: function to send email on given params (mailOptions) */
async function sendMail(mailOptions) {
    // console.log(" mailOption ", mailOptions);
    return new Promise(function(resolve, reject) {
        transporter.sendMail(mailOptions, function(err, info) {
            if (err) {
                console.log(" errors ", err)
                reject(0);
            }
            resolve(1);
        });
    })
    .then(null, (err) =>
        console.log(" error ", err)
    );
}
/* End Of: sendMail() */