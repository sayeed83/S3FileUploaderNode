var dbConfig = require('../../config/database');
const mysql = require('mysql');
let con = mysql.createConnection(dbConfig);
// console.log(" con ", con.on);
//- Reconnection function
function reconnect(con) {
    logger.info("\n New connection tentative...");

    //- Destroy the current connection variable
    if (con) con.destroy();

    //- Create a new one
    con = mysql.createConnection(dbConfig);

    //- Try to reconnect
    con.connect(function (err) {
        if (err) {
            console.log(" error ======== ", err);
            //- Try to connect every 2 seconds.
            setTimeout(reconnect, 2000);
        } else {
            logger.info("\n\t *** New connection established with the database. ***")
            return con;
        }
    });
}


var awsConfig = require('aws-config');
var AWS = require('aws-sdk');
var lambda = new AWS.Lambda({ region: process.env.AWS_REGION });
const parse = require('csv-parser');
const axios = require('axios');
var fs = require('fs');
const { ToWords } = require('to-words');
const PDFDocument = require('pdfkit');
const excel = require('node-excel-export');
// var logger = require('logger').createLogger('development.log');
const winston = require('winston');
const { createLogger, format, transports } = require('winston');
const { combine, timestamp, label, printf } = format;
var cron = require('node-cron');
var moment = require('moment');

const myFormat = printf(({ level, message, label, timestamp }) => {
    return `${moment(new Date()).format("YYYY-MM-DD HH:mm:ss")} ${message}`;
});
require('winston-daily-rotate-file');

var transport = new winston.transports.DailyRotateFile({
    filename: './logs/logfile.log',
    datePattern: 'YYYY-MM-DD-HH',
    zippedArchive: true,
    level: 'info',
    format: combine(
        timestamp(),
        myFormat
    ),
});

transport.on('rotate', function (oldFilename, newFilename) {
    logger.info(`oldFilename ${oldFilename}`);
    logger.info(`newFilename ${newFilename}`);
});

var logger = winston.createLogger({
    transports: [
        transport
    ]
});

const s3 = new AWS.S3({
    accessKeyId: process.env.ACCESS_KEY_ID,
    secretAccessKey: process.env.SECRET_ACCESS_KEY
});


var today = new Date();
var dd = String(today.getDate()).padStart(2, '0');
var mm = String(today.getMonth() + 1).padStart(2, '0');
var yyyy = today.getFullYear();
today = dd + '.' + mm + '.' + yyyy;

var afterSomeDate = new Date();
afterSomeDate.setDate(afterSomeDate.getDate() + 15);
var dd = String(afterSomeDate.getDate()).padStart(2, '0');
var mm = String(afterSomeDate.getMonth() + 1).padStart(2, '0'); //January is 0!
var yyyy = afterSomeDate.getFullYear();
afterSomeDate = dd + '.' + mm + '.' + yyyy;

exports.legalUpload = async (req, res) => {
    let response = {};
    let errors = [];
    let fileUploadData = [];
    let batchNumbers = [];
    let completedBatchNumber = [];
    let loggerObject = {};
    let correlationId = null;
    let channel = "WEB";
    let userId = null;
    let feature = null;
    let responseBody = null;
    try {
        logger.info(" came here ")
        fileUploadData = await getFileNameFormDb();
        let ids = [];
        for (let index = 0; index < fileUploadData.length; index++) {
            const element = fileUploadData[index];
            ids.push(fileUploadData[index].id)
        }
        logger.info(" ids ", ids);
        if (ids.length > 0) {
            await updateFileNameAsProcessing(ids);
        }
        // logger.info(" fileUploadData ", fileUploadData);
        logger.info(`fileUploadData ${fileUploadData.length}`);
        for (let index = 0; index < fileUploadData.length; index++) {
            const fileUpload = fileUploadData[index];
            correlationId = fileUpload.fileName;
            userId = fileUpload.userId;
            feature = fileUpload.operation;
            loggerObject = {
                userId,
                endpoint: '',
                feature: feature,
                method: "GET",
                request: JSON.stringify(fileUpload),
                createdBy: userId,
                updatedBy: userId,
                correlationId: correlationId,
                channel
            };
            loggerResponseCallback = await logRequest(loggerObject);
            let bucketName = '';
            if (fileUpload.operation == 'LEGAL_UPLOAD') {
                logger.info('if LEGAL_UPLOAD');
                // bucketName = process.env.DATA_IMPORT_BUCKET_NAME+'/'+process.env.LEGAL_NOTICE_BUCKET_NAME;
                bucketName = process.env.LEGAL_NOTICE_BUCKET_NAME;
            } else {
                logger.info('else LEGAL_UPLOAD');
                bucketName = process.env.DATA_IMPORT_BUCKET_NAME;
            }
            let params = { Bucket: bucketName, Key: fileUpload.fileName };
            let rawData = await getFile(params);
            await sendNotification(fileUpload, 'PROCESSING');
            logger.info(` ${fileUpload.operation} WITH BATCH NUMBER = ${fileUpload.id} STARTS HERE ${Date.now()} `);
            logger.info(`rawData ${rawData.length}`);
            if (fileUpload.operation == 'LOAN_UPLOADS') {
                const loanRawData = rawData;
                const loanRawDataCopy = rawData;
                logger.info(" loanRawData ", loanRawData);
                // return;
                let addressRawData = [];
                let addressRawDataForUpdate = [];
                let contactDetailsRawData = [];
                let contactDetailsRawDataForUpdate = [];
                let loanIdArray = [];
                let loanIdDetailsRawData = [];
                let loanFilterRawData = [];
                let contactRawData = [];
                let contactRawDataForUpdate = [];
                if (loanRawData.length > 0) {
                    for (let i = 0; i < loanRawData.length; i++) {
                        let contactObj = null;
                        let addressObj = null;
                        let contactDetailsObj = null;
                        let loanIdDetailsObj = null;

                        loanIdArray.push(loanRawData[i].loanId);
                        contactObj = {
                            loanId: loanRawData[i].loanId,
                            fullName: loanRawData[i].fullName,
                            relationship: 'SELF',
                            isPrimary: 'Y',
                            contactType: 'LOANEE',
                            startDate: loanRawData[i].agencyAllocationDate.split("/").reverse().join("-"),
                            createdBy: userId,
                            updatedBy: userId
                        }
                        contactRawData.push(contactObj);

                        addressObj = {
                            loanId: loanRawData[i].loanId,
                            address: loanRawData[i].address,
                            entity: 'CONTACT_ID',
                            addressType: loanRawData[i].addressType || 'HOME',
                            isPrimary: 'Y',
                            latitude: loanRawData[i].longitude,
                            longitude: loanRawData[i].longitude,
                            landmark: loanRawData[i].landmark,
                            line1: loanRawData[i].line1,
                            line2: loanRawData[i].line2,
                            roomNumber: loanRawData[i].roomNumber,
                            city: loanRawData[i].city,
                            district: loanRawData[i].district,
                            taluka: loanRawData[i].taluka,
                            state: loanRawData[i].state,
                            division: loanRawData[i].division,
                            pincode: loanRawData[i].pincode
                        }
                        // logger.info(` addressObj ${JSON.stringify(addressObj)}`)
                        addressRawData.push(addressObj);
                        // logger.info(` addressRawData ${JSON.stringify(addressRawData)}`)

                        if (loanRawData[i].address1) {
                            let temp = JSON.parse(JSON.stringify(addressObj));
                            temp.isPrimary = 'N';
                            temp.address = loanRawData[i].address1;
                            addressRawData.push(temp);
                        }
                        if (loanRawData[i].address2) {
                            let temp = JSON.parse(JSON.stringify(addressObj));
                            temp.isPrimary = 'N';
                            temp.address = loanRawData[i].address2;
                            addressRawData.push(temp);
                        }
                        contactDetailsObj = {};
                        if (loanRawData[i].mobile) {
                            contactDetailsObj.loanId = loanRawData[i].loanId;
                            contactDetailsObj.contactType = 'TELEPHONE';
                            contactDetailsObj.contactValue = loanRawData[i].mobile;
                            contactDetailsObj.isActive = 'Y';
                            contactDetailsObj.isPrimary = 'Y';
                            contactDetailsObj.createdBy = userId;
                            contactDetailsObj.updatedBy = userId;
                            contactDetailsObj.startDate = loanRawData[i].agencyAllocationDate.split("/").reverse().join("-");
                            logger.info(` contactDetailsObj ${JSON.stringify(contactDetailsObj)}`);
                            contactDetailsRawData.push(contactDetailsObj);
                            if (loanRawData[i].mobile1) {
                                let temp = JSON.parse(JSON.stringify(contactDetailsObj));
                                temp.isActive = 'N';
                                temp.isPrimary = 'N';
                                temp.contactValue = loanRawData[i].mobile1;
                                logger.info(` contactDetailsObj1 ${JSON.stringify(contactDetailsObj)}`);
                                contactDetailsRawData.push(temp);
                            }
                            if (loanRawData[i].mobile2) {
                                // contactDetailsObj.contactValue = loanRawData[i].mobile2;
                                let temp = JSON.parse(JSON.stringify(contactDetailsObj));
                                temp.isActive = 'N';
                                temp.isPrimary = 'N';
                                temp.contactValue = loanRawData[i].mobile2;
                                logger.info(` contactDetailsObj2 ${JSON.stringify(contactDetailsObj)}`);
                                contactDetailsRawData.push(temp);
                            }
                            if (loanRawData[i].mobile3) {
                                // contactDetailsObj.contactValue = loanRawData[i].mobile3;
                                logger.info(` contactDetailsObj3 ${JSON.stringify(contactDetailsObj)}`);
                                let temp = JSON.parse(JSON.stringify(contactDetailsObj));
                                temp.isActive = 'N';
                                temp.isPrimary = 'N';
                                temp.contactValue = loanRawData[i].mobile3;
                                contactDetailsRawData.push(temp);
                            }
                            if (loanRawData[i].mobile4) {
                                // contactDetailsObj.contactValue = loanRawData[i].mobile4;
                                logger.info(` contactDetailsObj4 ${JSON.stringify(contactDetailsObj)}`);
                                let temp = JSON.parse(JSON.stringify(contactDetailsObj));
                                temp.isActive = 'N';
                                temp.isPrimary = 'N';
                                temp.contactValue = loanRawData[i].mobile4;
                                contactDetailsRawData.push(temp);
                            }
                            if (loanRawData[i].mobile5) {
                                let temp = JSON.parse(JSON.stringify(contactDetailsObj));
                                temp.isActive = 'N';
                                temp.isPrimary = 'N';
                                temp.contactValue = loanRawData[i].mobile5;
                                contactDetailsRawData.push(temp);
                            }
                        }
                        logger.info(` contactDetailsRawData ${JSON.stringify(contactDetailsRawData)}`)
                        if (loanRawData[i].email) {
                            contactDetailsObj = null;
                            contactDetailsObj = {
                                loanId: loanRawData[i].loanId,
                                contactType: 'EMAIL',
                                contactValue: loanRawData[i].email,
                                isActive: 'Y',
                                isPrimary: 'Y',
                                createdBy: userId,
                                updatedBy: userId,
                                startDate: loanRawData[i].agencyAllocationDate.split("/").reverse().join("-")
                            }
                            contactDetailsRawData.push(contactDetailsObj);
                        }

                        loanIdDetailsObj = {
                            loanId: loanRawData[i].loanId,
                            agentId: loanRawData[i].agentId,
                            agencyId: loanRawData[i].agencyId,
                            financierId: loanRawData[i].financierId,
                            loanAmount: loanRawData[i].loanAmount,
                            principalOutStanding: loanRawData[i].principalOutStanding,
                            penalty: loanRawData[i].penalty,
                            totalOutStanding: loanRawData[i].totalOutStanding,
                            startDate: loanRawData[i].agencyAllocationDate.split("/").reverse().join("-"),
                            createdBy: userId,
                            updatedBy: userId
                        }
                        loanIdDetailsRawData.push(loanIdDetailsObj)

                        loanRawData[i].createdBy = userId
                        loanRawData[i].updatedBy = userId

                        delete loanRawData[i].fullName;
                        delete loanRawData[i].relationship;
                        delete loanRawData[i].email;
                        delete loanRawData[i].mobile;
                        delete loanRawData[i].isPrimary;
                        delete loanRawData[i].address;
                        delete loanRawData[i].addressType;
                        delete loanRawData[i].latitude;
                        delete loanRawData[i].longitude;
                        delete loanRawData[i].landmark;
                        delete loanRawData[i].line1;
                        delete loanRawData[i].line2;
                        delete loanRawData[i].roomNumber;
                        delete loanRawData[i].city;
                        delete loanRawData[i].district;
                        delete loanRawData[i].taluka;
                        delete loanRawData[i].state;
                        delete loanRawData[i].division;
                        delete loanRawData[i].pincode;
                        delete loanRawData[i].loanAmount;
                        delete loanRawData[i].principalOutStanding;
                        delete loanRawData[i].penalty;
                        delete loanRawData[i].totalOutStanding;
                        delete loanRawData[i].startDate;
                        delete loanRawData[i].endDate;
                        delete loanRawData[i].mobile1;
                        delete loanRawData[i].mobile2;
                        delete loanRawData[i].mobile3;
                        delete loanRawData[i].mobile4;
                        delete loanRawData[i].mobile5;
                        delete loanRawData[i].address1;
                        delete loanRawData[i].address2;

                        if (loanRawData[i].emiStartDate)
                            loanRawData[i].emiStartDate = loanRawData[i].emiStartDate.split("/").reverse().join("-");
                        if (loanRawData[i].agencyAllocationDate)
                            loanRawData[i].agencyAllocationDate = loanRawData[i].agencyAllocationDate.split("/").reverse().join("-");
                        if (loanRawData[i].maturityDate)
                            loanRawData[i].maturityDate = loanRawData[i].maturityDate.split("/").reverse().join("-");
                        if (loanRawData[i].emiDueDate)
                            loanRawData[i].emiDueDate = loanRawData[i].emiDueDate.split("/").reverse().join("-");
                        if (loanRawData[i].emiPaymentDate)
                            loanRawData[i].emiPaymentDate = loanRawData[i].emiPaymentDate.split("/").reverse().join("-");
                        if (loanRawData[i].loanDisbursementDate)
                            loanRawData[i].loanDisbursementDate = loanRawData[i].loanDisbursementDate.split("/").reverse().join("-");

                    }

                    let fetchLoanIdResult = await fetchLoanId(loanIdArray);
                    if (fetchLoanIdResult) {
                        // Remove duplicate loanIn from csv for Loanee table
                        for (let e = 0; e < loanRawDataCopy.length; e++) {
                            let checkDuplicateLoanId = fetchLoanIdResult.filter(loanIds => loanIds.loanId == loanRawDataCopy[e].loanId);

                            if (checkDuplicateLoanId.length == 0) {
                                // New data with filter for insert
                                loanFilterRawData.push(loanRawDataCopy[e]);
                            }
                            else {

                                let contactObj = null;
                                contactObj = {
                                    loanId: loanRawDataCopy[e].loanId,
                                    isPrimary: 'N',
                                    endDate: loanRawDataCopy[e].agencyAllocationDate.split("/").reverse().join("-"),
                                    updatedBy: userId
                                }
                                contactRawDataForUpdate.push(contactObj)

                                let contactDetailsObj = null;
                                contactDetailsObj = {
                                    loanId: loanRawDataCopy[e].loanId,
                                    isPrimary: 'N',
                                    endDate: loanRawDataCopy[e].agencyAllocationDate.split("/").reverse().join("-"),
                                    contactTypeIsMobile: loanRawDataCopy[e].mobile ? "Y" : "N",
                                    contactTypeIsEmail: loanRawDataCopy[e].email ? "Y" : "N",
                                    updatedBy: userId
                                }
                                contactDetailsRawDataForUpdate.push(contactDetailsObj);

                                let addressObj = null;
                                addressObj = {
                                    loanId: loanRawDataCopy[e].loanId,
                                    isPrimary: 'N',
                                    endDate: loanRawDataCopy[e].agencyAllocationDate.split("/").reverse().join("-"),
                                    updatedBy: userId
                                }
                                addressRawDataForUpdate.push(addressObj)
                            }
                        }

                    }
                    else {
                        loanFilterRawData = loanRawData;
                    }

                    let getContactDataForUpdateContactDetails = await getContact(loanIdArray);
                    if (fetchLoanIdResult) {
                        await updateContact(contactRawDataForUpdate, 'CONTACT');
                    }
                    console.log(" loanFilterRawData ", loanFilterRawData);
                    // logger.info(` loanFilterRawData ${JSON.stringify(loanFilterRawData)}`);
                    await insertToDB(loanFilterRawData, 'LOANEE');
                    await insertToDB(contactRawData, 'CONTACT');
                    await insertToDB(loanIdDetailsRawData, 'LOAN_DEATLS');
                    let getContactData = await getContact(loanIdArray);
                    logger.info(` addressRawData Before `);
                    logger.info(" addressRawData Before ", addressRawData);
                    logger.info(` contactDetailsRawData Before ${JSON.stringify(contactDetailsRawData)}`);
                    logger.info(" contactDetailsRawData Before ", contactDetailsRawData);
                    for (let c = 0; c < getContactData.length; c++) {
                        for (let a = 0; a < addressRawData.length; a++) {
                            if (addressRawData[a].loanId == getContactData[c].loanId) {
                                addressRawData[a].value = getContactData[c].contactId
                                addressRawData[a].createdBy = userId
                                addressRawData[a].updatedBy = userId
                                delete addressRawData[a].loanId
                            }
                        }
                        for (let d = 0; d < contactDetailsRawData.length; d++) {
                            if (contactDetailsRawData[d].loanId == getContactData[c].loanId) {
                                contactDetailsRawData[d].contactId = getContactData[c].contactId
                                contactDetailsRawData[d].createdBy = userId
                                contactDetailsRawData[d].updatedBy = userId
                                delete contactDetailsRawData[d].loanId
                            }
                        }

                    }

                    logger.info(` addressRawData After ${JSON.stringify(addressRawData)}`);
                    logger.info(" addressRawData After ", addressRawData);
                    logger.info(` contactDetailsRawData After ${JSON.stringify(contactDetailsRawData)}`);
                    logger.info(" contactDetailsRawData After ", contactDetailsRawData);

                    for (var m = 0; m < getContactDataForUpdateContactDetails.length; m++) {
                        for (let n = 0; n < contactDetailsRawDataForUpdate.length; n++) {
                            if (contactDetailsRawDataForUpdate[n].loanId == getContactDataForUpdateContactDetails[m].loanId) {
                                contactDetailsRawDataForUpdate[n].contactId = getContactDataForUpdateContactDetails[m].contactId
                            }
                        }
                        for (let o = 0; o < addressRawDataForUpdate.length; o++) {
                            if (addressRawDataForUpdate[o].loanId == getContactDataForUpdateContactDetails[m].loanId) {
                                addressRawDataForUpdate[o].contactId = getContactDataForUpdateContactDetails[m].contactId
                            }
                        }
                    }
                    if (fetchLoanIdResult) {
                        await updateContactDetails(contactDetailsRawDataForUpdate, 'CONTACT_DETAILS');
                        await updateAddress(addressRawDataForUpdate, 'ADDRESS');
                    }
                    await insertToDB(addressRawData, 'ADDRESS');
                    await insertToDB(contactDetailsRawData, 'CONTACT_DETAILS');
                    message = "Uploaded Sucessfully";
                }
            }
            else if (fileUpload.operation == 'DISPOSITION') {
                const dispositionRawData = rawData;
                if (dispositionRawData.length > 0) {
                    for (let i = 0; i < dispositionRawData.length; i++) {
                        dispositionRawData[i].updatedBy = userId
                        dispositionRawData[i].createdBy = userId
                        dispositionRawData[i].channel = "WEB"
                        dispositionRawData[i].dispositionDate = dispositionRawData[i].dispositionDate.split("/").reverse().join("-")
                    }
                }
                await insertToDB(dispositionRawData, 'LOAN_TRANSACTION');
                message = "Uploaded Sucessfully";
            }
            else if (fileUpload.operation == 'PAIDFILE_UPLOAD') {
                let paidFileData = [];
                for (var i = 0; i < rawData.length; i++) {
                    let temp = {};
                    let getLoanDetailDataResult = await getLoanDetailsData(rawData[i].loanId);
                    if (getLoanDetailDataResult) {

                        temp.loanId = rawData[i].loanId;
                        temp.financierId = rawData[i].financier;
                        temp.agencyId = getLoanDetailDataResult.agencyId;
                        temp.totalAmount = rawData[i].amountCollected;
                        temp.dispositionCode = 'PAID';
                        temp.dispositionStatus = 'CLOSED';
                        temp.dispositionDate = rawData[i].collectedDate.split("/").reverse().join("-");
                        temp.channel = rawData[i].paymentMode.toUpperCase();
                        temp.updatedBy = userId;
                        temp.createdBy = userId;
                        temp.agentId = userId;
                        temp.duplicateEntry = 0;
                        let getLoanDatas = await getLoanData(rawData[i].loanId, rawData[i].amountCollected, (temp.dispositionDate).replace(/\s/g, ''));
                        if (getLoanDatas.length > 0) {
                            temp.duplicateEntry = 1;
                        }
                        paidFileData.push(temp);
                    }
                }
                logger.info(" paidFileData ", paidFileData);
                await insertToDB(paidFileData, 'LOAN_TRANSACTION', 'INSERT');
            }
            else if (fileUpload.operation == 'EXPORT_DATA') {
                let allFinalResponse = {};
                console.log(" rawData[i] ===== ", rawData[0]);
                // return;
                for (var i = 0; i < rawData.length; i++) {
                    body = rawData[i];
                    userId = rawData[i].userId

                    loansResponse = [];
                    loansResponseCount = await getExpOrtdataCount(body);
                    logger.info(`loansResponseCount ${Math.ceil(loansResponseCount[0].count)}`);
                    // return;
                    let offset = 0;
                    let limit = 200;
                    for (let index = 0; index < Math.ceil(loansResponseCount[0].count / 200); index++) {
                        // loansResponse = [];
                        // const element = array[index];
                        logger.info(`offset  ${offset} && limit = ${limit}`);
                        loansResponse = await getExpOrtdata(body, offset, limit);
                        offset = offset + 200;
                        if (offset == limit) {
                            offset = offset + 200;
                        }
                        finalResponse = {};

                        for (var j = 0; j < loansResponse.length; j++) {
                            let getLoanDetailsDatas = await getLoanDetailsData(loansResponse[j].loanId);
                            loansResponse[j].agencyId = getLoanDetailsDatas ? getLoanDetailsDatas.agencyId : loansResponse[j].agencyId;
                            loansResponse[j].agentId = getLoanDetailsDatas ? getLoanDetailsDatas.agentId : loansResponse[j].agentId;
                            loansResponse[j].financierId = getLoanDetailsDatas ? getLoanDetailsDatas.financierId : loansResponse[j].financierId;
                            loansResponse[j].loanAmount = getLoanDetailsDatas ? getLoanDetailsDatas.loanAmount : loansResponse[j].loanAmount;
                            loansResponse[j].outstanding = getLoanDetailsDatas ? getLoanDetailsDatas.outstanding : loansResponse[j].outstanding;
                            loansResponse[j].emi = getLoanDetailsDatas ? getLoanDetailsDatas.emi : loansResponse[j].emi;
                            loansResponse[j].due = getLoanDetailsDatas ? getLoanDetailsDatas.due : loansResponse[j].due;

                        }

                        let agencyNameResponse = await getAgencyFinancer("AGENCY");
                        let financierNameResponse = await getAgencyFinancer("FINANCIER");
                        let arrayOfLoanId = loansResponse.map((loan) => loan.loanId);
                        let contactResponse = await getContact(arrayOfLoanId);
                        let arrayOfContactId = contactResponse.map((contact) => contact.contactId);
                        let contactDetailsResponse = await getContactDetail(arrayOfContactId);
                        let contactAddressResponse = await getContactAddress(arrayOfContactId);
                        let loanDetailsResponse = await getLoanDetails(arrayOfLoanId, body.downloadFileName);
                        let primaryContact = [],
                            alternateContacts = [],
                            contact = {},
                            contactsArray = [];
                        primaryContact = contactResponse.filter((contact) => contact.isPrimary === "Y");
                        alternateContacts = contactResponse.filter((contact) => contact.isPrimary !== "Y");

                        for (let ldr = 0; ldr < loanDetailsResponse.length; ldr++) {
                            let agentName = [];
                            if (Number.isInteger(loanDetailsResponse[ldr].agentId)) {
                                agentName = await getUserData(loanDetailsResponse[ldr].agentId);
                            }

                            loanDetailsResponse[ldr].agentName = agentName[0] ? agentName[0].fullName : '';
                            for (let a = 0; a < agencyNameResponse.length; a++) {
                                if (loanDetailsResponse[ldr].agencyId == agencyNameResponse[a].id)
                                    loanDetailsResponse[ldr].agencyName = agencyNameResponse[a].name;
                            }
                        }

                        for (let k = 0; k < loansResponse.length; k++) {
                            if (loansResponse[k].agentId != 0) {
                                let agentName = [];
                                if (Number.isInteger(loansResponse[k].agentId)) {
                                    agentName = await getUserData(loansResponse[k].agentId);
                                }
                                loansResponse[k].agentName = agentName[0] ? agentName[0].fullName : "Not defined";
                            }
                            else {
                                loansResponse[k].agentName = "Not defined";
                            }
                            for (let a = 0; a < agencyNameResponse.length; a++) {
                                if (loansResponse[k].agencyId == agencyNameResponse[a].id)
                                    loansResponse[k].agencyName = agencyNameResponse[a].name;
                            }
                            for (let a = 0; a < financierNameResponse.length; a++) {
                                if (loansResponse[k].financierId == financierNameResponse[a].id)
                                    loansResponse[k].financierName = financierNameResponse[a].name;
                            }
                        }
                        for (let i = 0; i < primaryContact.length; i++) {
                            contact = {};
                            const loanId = primaryContact[i].loanId;
                            const primaryContactId = primaryContact[i].contactId;
                            contact.clientId = primaryContactId;
                            contact.fullName = primaryContact[i].fullName;
                            // contact.lastName = primaryContact[i].lastName;
                            contact.image = primaryContact[i].image;
                            contact.alternateContacts = [];
                            contact.contactDetails = {};
                            for (let j = 0; j < alternateContacts.length; j++) {
                                if (loanId === alternateContacts[j].loanId) {
                                    contact.alternateContacts.push({
                                        clientId: alternateContacts[j].clientId,
                                        fullName: alternateContacts[j].full,
                                        //lastName: alternateContacts[j].lastName,
                                        image: alternateContacts[j].image,
                                        relationship: alternateContacts[j].relationship,
                                    });
                                }
                            }
                            for (let k = 0; k < loansResponse.length; k++) {
                                if (loanId === loansResponse[k].loanId) {
                                    contact.loan = {
                                        ...loansResponse[k],
                                        disposition: [],
                                    };
                                    for (let l = 0; l < loanDetailsResponse.length; l++) {
                                        if (loanId === loanDetailsResponse[l].loanId) {
                                            const dispositionDate = JSON.stringify(
                                                loanDetailsResponse[l].dispositionDate
                                            );
                                            if (dispositionDate.length > 10)
                                                loanDetailsResponse[
                                                    l
                                                ].dispositionDate = dispositionDate.substr(1, 10);
                                            if (
                                                loanDetailsResponse[l].dispositionCode == "PTP" &&
                                                loanDetailsResponse[l].dispositionStatus == "OPEN"
                                            ) {
                                                let dispositionDate1 = JSON.stringify(
                                                    loanDetailsResponse[l].dispositionDate
                                                );
                                                contact.loan.ptpDate = dispositionDate1.substr(1, 10);
                                            }
                                            contact.loan.disposition.push({ ...loanDetailsResponse[l] });
                                        }
                                    }
                                }
                            }
                            contact.contactDetails.telephone = [];
                            contact.contactDetails.email = [];
                            contact.contactDetails.others = [];
                            contact.contactDetails.address = [];
                            for (let m = 0; m < contactDetailsResponse.length; m++) {
                                if (primaryContactId === contactDetailsResponse[m].contactId) {
                                    if (contactDetailsResponse[m].contactType === "TELEPHONE")
                                        contact.contactDetails.telephone.push({
                                            ...contactDetailsResponse[m],
                                        });
                                    else if (contactDetailsResponse[m].contactType === "EMAIL")
                                        contact.contactDetails.email.push({
                                            ...contactDetailsResponse[m],
                                        });
                                    else
                                        contact.contactDetails.others.push({
                                            ...contactDetailsResponse[m],
                                        });
                                }
                            }
                            for (let n = 0; n < contactAddressResponse.length; n++) {
                                if (primaryContactId == contactAddressResponse[n].value) {
                                    contact.contactDetails.address.push({
                                        ...contactAddressResponse[n],
                                    });
                                }
                            }
                            contactsArray.push(contact);
                        }
                        finalResponse = contactsArray;
                        Array.prototype.push.apply(allFinalResponse, finalResponse);
                        logger.info(`allFinalResponse inside ====== ${allFinalResponse.length}`);
                    }
                }
                logger.info(`allFinalResponse out side ====== ${allFinalResponse.length}`);
                logger.info(` after get finalResponse  ${Date.now()} `)
                // You can define styles as json object
                const styles = {
                    headerDark: {
                        fill: {
                            fgColor: {
                                rgb: 'FF000000'
                            },
                            color: {
                                rgb: 'FFFFFFFF'
                            },
                        },
                        font: {
                            color: {
                                rgb: 'FFFFFFFF'
                            },
                            sz: 14,
                            bold: true,
                            underline: true
                        }
                    }
                };

                //Here you specify the export structure
                const loanSpec = {
                    clientId: { // <- the key should match the actual data key
                        displayName: 'Client Id', // <- Here you specify the column header
                        headerStyle: styles.headerDark, // <- Header style
                    },
                    fullName: {
                        displayName: 'Full Name',
                        headerStyle: styles.headerDark,
                    },
                    Image: {
                        displayName: 'Image',
                        headerStyle: styles.headerDark,
                    },
                    loanId: {
                        displayName: 'Loan Id',
                        headerStyle: styles.headerDark,
                    },
                    agencyId: {
                        displayName: 'Agency Id',
                        headerStyle: styles.headerDark,
                    },
                    agencyName: {
                        displayName: 'Agency Name',
                        headerStyle: styles.headerDark,
                    },
                    agentId: {
                        displayName: 'Agent Id',
                        headerStyle: styles.headerDark,
                    },
                    agentName: {
                        displayName: 'Agent Name',
                        headerStyle: styles.headerDark,
                    },
                    agencyAllocationDate: {
                        displayName: 'Agency Allocation Date',
                        headerStyle: styles.headerDark,
                    },
                    allocationDPD: {
                        displayName: 'Allocation DPD',
                        headerStyle: styles.headerDark,
                    },
                    category: {
                        displayName: 'Category',
                        headerStyle: styles.headerDark,
                    },
                    due: {
                        displayName: 'Due',
                        headerStyle: styles.headerDark,
                    },
                    emi: {
                        displayName: 'EMI',
                        headerStyle: styles.headerDark,
                    },
                    emiPaymentDate: {
                        displayName: 'EMI Payment Date',
                        headerStyle: styles.headerDark,
                    },
                    financierId: {
                        displayName: 'Financier Id',
                        headerStyle: styles.headerDark,
                    },
                    financierName: {
                        displayName: 'Financier Name',
                        headerStyle: styles.headerDark,
                    },
                    loanType: {
                        displayName: 'Loan Type',
                        headerStyle: styles.headerDark,
                    },
                    loanAmount: {
                        displayName: 'Loan Amount',
                        headerStyle: styles.headerDark,
                    },
                    loanTenure: {
                        displayName: 'Loan Tenure',
                        headerStyle: styles.headerDark,
                    },
                    outstanding: {
                        displayName: 'Outstanding',
                        headerStyle: styles.headerDark,
                    },
                    maturityDate: {
                        displayName: 'Maturity Date',
                        headerStyle: styles.headerDark,
                    },
                    mandateStatus: {
                        displayName: 'Mandate Status',
                        headerStyle: styles.headerDark,
                    },
                    ptpDate: {
                        displayName: 'PTP Date',
                        headerStyle: styles.headerDark,
                    },
                    referenceLoanId: {
                        displayName: 'Reference Loan Id',
                        headerStyle: styles.headerDark,
                    },
                }

                const dipostionSepec = {
                    clientId: { // <- the key should match the actual data key
                        displayName: 'Client Id', // <- Here you specify the column header
                        headerStyle: styles.headerDark, // <- Header style
                    },
                    fullName: {
                        displayName: 'Full Name',
                        headerStyle: styles.headerDark,
                    },
                    Image: {
                        displayName: 'Image',
                        headerStyle: styles.headerDark,
                    },
                    id: {
                        displayName: 'Id',
                        headerStyle: styles.headerDark,
                    },
                    loanId: {
                        displayName: 'Loan Id',
                        headerStyle: styles.headerDark,
                    },
                    agencyId: {
                        displayName: 'Agency Id',
                        headerStyle: styles.headerDark,
                    },
                    agencyName: {
                        displayName: 'Agency Name',
                        headerStyle: styles.headerDark,
                    },
                    agentId: {
                        displayName: 'Agent Id',
                        headerStyle: styles.headerDark,
                    },
                    agentName: {
                        displayName: 'Agent Name',
                        headerStyle: styles.headerDark,
                    },
                    attachment1: {
                        displayName: 'Attachment1',
                        headerStyle: styles.headerDark,
                    },
                    channel: {
                        displayName: 'Channel',
                        headerStyle: styles.headerDark,
                    },
                    collectionDate: {
                        displayName: 'Collection Date',
                        headerStyle: styles.headerDark,
                    },
                    contactNumber: {
                        displayName: 'Contact Number',
                        headerStyle: styles.headerDark,
                    },
                    createdAt: {
                        displayName: 'Created At',
                        headerStyle: styles.headerDark,
                    },
                    createdBy: {
                        displayName: 'Created By',
                        headerStyle: styles.headerDark,
                    },
                    dispositionCode: {
                        displayName: 'Disposition Code',
                        headerStyle: styles.headerDark,
                    },
                    dispositionDate: {
                        displayName: 'Disposition Date',
                        headerStyle: styles.headerDark,
                    },
                    dispositionStatus: {
                        displayName: 'Disposition Status',
                        headerStyle: styles.headerDark,
                    },
                    email: {
                        displayName: 'Email',
                        headerStyle: styles.headerDark,
                    },
                    emiAmount: {
                        displayName: 'EMI Amount',
                        headerStyle: styles.headerDark,
                    },
                    latitude: {
                        displayName: 'Latitude',
                        headerStyle: styles.headerDark,
                    },
                    longitude: {
                        displayName: 'Longitude',
                        headerStyle: styles.headerDark,
                    },
                    penaltyAmount: {
                        displayName: 'Penalty Amount',
                        headerStyle: styles.headerDark,
                    },
                    remark: {
                        displayName: 'Remark',
                        headerStyle: styles.headerDark,
                    },
                    totalAmount: {
                        displayName: 'Total Amount',
                        headerStyle: styles.headerDark,
                    },
                    updatedAt: {
                        displayName: 'Updated At',
                        headerStyle: styles.headerDark,
                    },
                    updatedBy: {
                        displayName: 'Updated By',
                        headerStyle: styles.headerDark,
                    },
                }

                const contactDetailsAddressSpec = {
                    clientId: { // <- the key should match the actual data key
                        displayName: 'Client Id', // <- Here you specify the column header
                        headerStyle: styles.headerDark, // <- Header style
                    },
                    fullName: {
                        displayName: 'Full Name',
                        headerStyle: styles.headerDark,
                    },
                    Image: {
                        displayName: 'Image',
                        headerStyle: styles.headerDark,
                    },
                    loanId: {
                        displayName: 'Loan Id',
                        headerStyle: styles.headerDark,
                    },
                    agencyId: {
                        displayName: 'Agency Id',
                        headerStyle: styles.headerDark,
                    },
                    agentId: {
                        displayName: 'Agent Id',
                        headerStyle: styles.headerDark,
                    },
                    address: {
                        displayName: 'Address',
                        headerStyle: styles.headerDark,
                    },
                    addressId: {
                        displayName: 'Address Id',
                        headerStyle: styles.headerDark,
                    },
                    AddressType: {
                        displayName: 'Address Type',
                        headerStyle: styles.headerDark,
                    },
                    city: {
                        displayName: 'City',
                        headerStyle: styles.headerDark,
                    },
                    district: {
                        displayName: 'District',
                        headerStyle: styles.headerDark,
                    },
                    division: {
                        displayName: 'Division',
                        headerStyle: styles.headerDark,
                    },
                    isPrimary: {
                        displayName: 'IsPrimary',
                        headerStyle: styles.headerDark,
                    },
                    landmark: {
                        displayName: 'Landmark',
                        headerStyle: styles.headerDark,
                    },
                    latitude: {
                        displayName: 'Latitude',
                        headerStyle: styles.headerDark,
                    },
                    longitude: {
                        displayName: 'Longitude',
                        headerStyle: styles.headerDark,
                    },
                    line1: {
                        displayName: 'Line1',
                        headerStyle: styles.headerDark,
                    },
                    line2: {
                        displayName: 'Line2',
                        headerStyle: styles.headerDark,
                    },
                    pincode: {
                        displayName: 'Pin Code',
                        headerStyle: styles.headerDark,
                    },
                    roomNumber: {
                        displayName: 'Room Number',
                        headerStyle: styles.headerDark,
                    },
                    state: {
                        displayName: 'State',
                        headerStyle: styles.headerDark,
                    },
                    taluka: {
                        displayName: 'Taluka',
                        headerStyle: styles.headerDark,
                    },
                    value: {
                        displayName: 'Value',
                        headerStyle: styles.headerDark,
                    },
                }

                const contactDetailSepec = {
                    clientId: { // <- the key should match the actual data key
                        displayName: 'Client Id', // <- Here you specify the column header
                        headerStyle: styles.headerDark, // <- Header style
                    },
                    fullName: {
                        displayName: 'Full Name',
                        headerStyle: styles.headerDark,
                    },
                    image: {
                        displayName: 'Image',
                        headerStyle: styles.headerDark,
                    },
                    loanId: {
                        displayName: 'Loan Id',
                        headerStyle: styles.headerDark,
                    },
                    agencyId: {
                        displayName: 'Agency Id',
                        headerStyle: styles.headerDark,
                    },
                    agentId: {
                        displayName: 'Agent Id',
                        headerStyle: styles.headerDark,
                    },
                    contactId: {
                        displayName: 'Contact Id',
                        headerStyle: styles.headerDark,
                    },
                    contactDetailId: {
                        displayName: 'Contact Detail Id',
                        headerStyle: styles.headerDark,
                    },
                    contactType: {
                        displayName: 'Contact Type',
                        headerStyle: styles.headerDark,
                    },
                    contactValue: {
                        displayName: 'Contact Value',
                        headerStyle: styles.headerDark,
                    },
                    isPrimary: {
                        displayName: 'Is Primary',
                        headerStyle: styles.headerDark,
                    },
                    isActive: {
                        displayName: 'Is Active',
                        headerStyle: styles.headerDark,
                    }
                }

                const loandataset = [];
                const dipostiondataset = [];
                const contactDetailsAddressdataset = [];
                const contactDetaildataset = [];
                logger.info(` starting with csv file ${Date.now()}`)
                for (var i = 0; i < allFinalResponse.length; i++) {
                    let data = {};
                    data.clientId = allFinalResponse[i].clientId;
                    data.fullName = allFinalResponse[i].fullName;
                    data.Image = allFinalResponse[i].Image;
                    data.loanId = allFinalResponse[i].loan.loanId;
                    data.agencyId = allFinalResponse[i].loan.agencyId;
                    data.agencyName = allFinalResponse[i].loan.agencyName;
                    data.agentId = allFinalResponse[i].loan.agentId;
                    data.agentName = allFinalResponse[i].loan.agentName;
                    data.agencyAllocationDate = allFinalResponse[i].loan.agencyAllocationDate;
                    data.allocationDPD = allFinalResponse[i].loan.allocationDPD;
                    data.category = allFinalResponse[i].loan.category;
                    data.due = allFinalResponse[i].loan.due;
                    data.emi = allFinalResponse[i].loan.emi;
                    data.emiPaymentDate = allFinalResponse[i].loan.emiPaymentDate;
                    data.financierId = allFinalResponse[i].loan.financierId;
                    data.financierName = allFinalResponse[i].loan.financierName;
                    data.loanType = allFinalResponse[i].loan.loanType;
                    data.loanAmount = allFinalResponse[i].loan.loanAmount;
                    data.loanTenure = allFinalResponse[i].loan.loanTenure;
                    data.outstanding = allFinalResponse[i].loan.outstanding;
                    data.maturityDate = allFinalResponse[i].loan.maturityDate;
                    data.mandateStatus = allFinalResponse[i].loan.mandateStatus;
                    data.ptpDate = allFinalResponse[i].loan.ptpDate ? allFinalResponse[i].loan.ptpDate : '';
                    data.referenceLoanId = allFinalResponse[i].loan.referenceLoanId;
                    loandataset.push(data);

                    // Disposition data starts here.
                    let dispositionArrayObj = allFinalResponse[i].loan.disposition;
                    for (let dao = 0; dao < dispositionArrayObj.length; dao++) {
                        const element = dispositionArrayObj[dao];
                        let dispositionData = {};
                        dispositionData.clientId = allFinalResponse[i].clientId;
                        dispositionData.fullName = allFinalResponse[i].fullName;
                        dispositionData.Image = allFinalResponse[i].Image;
                        dispositionData.id = element.id;
                        dispositionData.loanId = element.loanId;
                        dispositionData.agencyId = element.agencyId;
                        dispositionData.agencyName = element.agencyName;
                        dispositionData.agentId = element.agentId;
                        dispositionData.agentName = element.agentName;
                        dispositionData.attachment1 = element.attachment1;
                        dispositionData.collectionDate = element.collectionDate;
                        dispositionData.contactNumber = element.contactNumber;
                        dispositionData.createdAt = element.createdAt;
                        dispositionData.createdBy = element.createdBy;
                        dispositionData.dispositionCode = element.dispositionCode;
                        dispositionData.dispositionDate = element.dispositionDate;
                        dispositionData.dispositionStatus = element.dispositionStatus;
                        dispositionData.email = element.email;
                        dispositionData.emiAmount = element.emiAmount;
                        dispositionData.latitude = element.latitude;
                        dispositionData.longitude = element.longitude;
                        dispositionData.penaltyAmount = element.penaltyAmount;
                        dispositionData.remark = element.remark;
                        dispositionData.totalAmount = element.totalAmount;
                        dispositionData.updatedAt = element.updatedAt;
                        dispositionData.updatedBy = element.updatedBy;
                        dipostiondataset.push(dispositionData);
                    }

                    // Contact Details address data starts here
                    let addressObj = allFinalResponse[i].contactDetails.address;
                    for (let aobj = 0; aobj < addressObj.length; aobj++) {
                        let element = addressObj[aobj];
                        element.clientId = allFinalResponse[i].clientId;
                        element.fullName = allFinalResponse[i].fullName;
                        element.Image = allFinalResponse[i].Image;
                        element.loanId = allFinalResponse[i].loan.loanId;
                        element.agencyId = allFinalResponse[i].loan.agencyId;
                        element.agentId = allFinalResponse[i].loan.agentId;
                        contactDetailsAddressdataset.push(element)
                    }

                    // Contact Details data starts here
                    let telephoneObj = allFinalResponse[i].contactDetails.telephone;
                    for (let tobj = 0; tobj < telephoneObj.length; tobj++) {
                        let element = telephoneObj[tobj];
                        element.clientId = allFinalResponse[i].clientId;
                        element.fullName = allFinalResponse[i].fullName;
                        element.Image = allFinalResponse[i].Image;
                        element.loanId = allFinalResponse[i].loan.loanId;
                        element.agencyId = allFinalResponse[i].loan.agencyId;
                        element.agentId = allFinalResponse[i].loan.agentId;
                        contactDetaildataset.push(element)
                    }
                }

                logger.info(`after with csv file ${Date.now()}`)

                // Create the excel report.
                // This function will return Buffer
                const report = excel.buildExport([
                    {
                        name: 'Loan', // <- Specify sheet name (optional)
                        specification: loanSpec, // <- Report specification
                        data: loandataset // <-- Report data
                    },
                    {
                        name: 'Disposition',
                        specification: dipostionSepec,
                        data: dipostiondataset
                    },
                    {
                        name: 'Constact Details Address',
                        specification: contactDetailsAddressSpec,
                        data: contactDetailsAddressdataset
                    },
                    {
                        name: 'Constact Details',
                        specification: contactDetailSepec,
                        data: contactDetaildataset
                    }
                ]);

                const params = {
                    Bucket: process.env.DATA_IMPORT_BUCKET_NAME,
                    Key: body.fileName,
                    Body: report,
                };
                logger.info(`params ======= ${params}`);
                const result = await s3.putObject(params).promise();
                logger.info(` result ${result}`);
                body.totalRecord = allFinalResponse.length;
                await updateIntoDataExport(body, fileUpload.id);
                logger.info(` end ${Date.now()} `)
            }
            else if (fileUpload.operation == 'DOWNLOAD_PAYMENT_FILE') {
                let allFinalResponse = {}
                console.log(" rawData[0] ", rawData[0]);
                // return;
                let userId = rawData[0].userId;
                let startDate = rawData[0].startDate;
                let endDate = rawData[0].endDate;
                let fileName = rawData[0].fileName;
                body = rawData[0];
                let paymentData = await getPaymentData(rawData[0]);
                let paymentDataFinal = [];
                // console.log(" paymentData.length ", paymentData.length);
                for (let i = 0; i < paymentData.length; i++) {
                    let loanDetailData = await getLoanDetailsData(paymentData[i].loanId);
                    if(body.exportType == 'FINANCIER') {
                        paymentData[i]["finacierId"] = loanDetailData.financierId;
                        if(body.exportTypeId == loanDetailData.financierId && body.exportTypeId != '0') {
                            paymentDataFinal.push(paymentData[i]);
                        } 
                        else {
                            paymentDataFinal.push(paymentData[i]);
                        }
                        
                    }
                    else if(body.exportType == 'USER') {
                        if(body.exportTypeId == paymentData[i].agentId && body.exportTypeId != '0') {
                            paymentDataFinal.push(paymentData[i]);
                        } else {
                            paymentDataFinal.push(paymentData[i]);
                        }
                    }
                    else if(body.exportType == 'AGENCY') {
                        if(body.exportTypeId == paymentData[i].agencyId && body.exportTypeId != '0') {
                            paymentDataFinal.push(paymentData[i]);
                        } else {
                            paymentDataFinal.push(paymentData[i]);
                        }
                    }
                    
                }
                console.log(" paymentDataFinal ", paymentDataFinal);
                console.log(" paymentData ", paymentData.length);
                // return;
                
                // logger.info(`paymentData ====== ${paymentData}`);
                logger.info(` after get finalResponse  ${Date.now()} `);
                // You can define styles as json object
                const styles = {
                    headerDark: {
                        fill: {
                            fgColor: {
                                rgb: 'FF000000'
                            },
                            color: {
                                rgb: 'FFFFFFFF'
                            },
                        },
                        font: {
                            color: {
                                rgb: 'FFFFFFFF'
                            },
                            sz: 14,
                            bold: true,
                            underline: true
                        }
                    }
                };

                //Here you specify the export structure
                const loanSpec = {
                    loanId: { // <- the key should match the actual data key
                        displayName: 'Loan Id', // <- Here you specify the column header
                        headerStyle: styles.headerDark, // <- Header style
                    },
                    paymentAmount: {
                        displayName: 'Payment Amount',
                        headerStyle: styles.headerDark,
                    },
                    paymentMode: {
                        displayName: 'Payment Mode',
                        headerStyle: styles.headerDark,
                    },
                    paymentDateTime: {
                        displayName: 'Date Time',
                        headerStyle: styles.headerDark,
                    },
                    agencyId: {
                        displayName: 'Agency Name',
                        headerStyle: styles.headerDark,
                    },
                    agencyName: {
                        displayName: 'Agency Name',
                        headerStyle: styles.headerDark,
                    },
                    financierName: {
                        displayName: 'Financier Name',
                        headerStyle: styles.headerDark,
                    },
                    customerName: {
                        displayName: 'Customer Name',
                        headerStyle: styles.headerDark,
                    }
                }

                const loandataset = [];
                logger.info(` starting with csv file ${Date.now()}`)
                for (var i = 0; i < paymentDataFinal.length; i++) {
                    let data = {};
                    data.loanId = paymentDataFinal[i].loanId;
                    data.paymentAmount = paymentDataFinal[i].paymentAmount;
                    data.paymentMode = paymentDataFinal[i].paymentMode;
                    data.paymentDateTime = paymentDataFinal[i].paymentDateTime;
                    data.agencyName = paymentDataFinal[i].agencyName;
                    data.customerName = paymentDataFinal[i].customerName;
                    data.financierName = paymentDataFinal[i].financierName;
                    loandataset.push(data);
                }

                logger.info(`after with csv file ${Date.now()}`)

                // Create the excel report.
                // This function will return Buffer
                const report = excel.buildExport([
                    {
                        name: 'PaymentFile', // <- Specify sheet name (optional)
                        specification: loanSpec, // <- Report specification
                        data: loandataset // <-- Report data
                    }
                ]);

                const params = {
                    Bucket: process.env.DATA_IMPORT_BUCKET_NAME,
                    Key: fileName,
                    Body: report,
                };
                logger.info(`params ======= ${params}`);
                const result = await s3.putObject(params).promise();
                logger.info(` result ${result}`);
                body.totalRecord = paymentDataFinal.length;
                // body.exportTypeName = '';
                await updateIntoDataExport(body, fileUpload.id);
                logger.info(` end ${Date.now()} `)
            }
            else if (fileUpload.operation == 'DOWNLOAD_LEGAL_NOTICE') {
                let allFinalResponse = {}
                console.log(" rawData[0] ", rawData[0]);
                // return;
                let userId = rawData[0].userId;
                let startDate = rawData[0].startDate;
                let endDate = rawData[0].endDate;
                let fileName = rawData[0].fileName;
                body = rawData[0];

                let legalNoticeSentData = await getLegalNoticeData(rawData[0]);
                let legalNoticeSentDataFinal = [];
                // console.log(" legalNoticeSentData.legalNoticeSentData ", legalNoticeSentData.legalNoticeSentData);
                for (let i = 0; i < legalNoticeSentData.length; i++) {
                    let loanDetailData = await getLoanDetailsData(legalNoticeSentData[i].loanId);
                    // console.log(" loanDetailData === ", loanDetailData);
                    // return;
                    if(body.exportType == 'FINANCIER') {
                        legalNoticeSentData[i]["finacierId"] = loanDetailData.financierId;
                        if(body.exportTypeId == loanDetailData.financierId && body.exportTypeId != '0') {
                            legalNoticeSentDataFinal.push(legalNoticeSentData[i]);
                        } 
                        else {
                            legalNoticeSentDataFinal.push(legalNoticeSentData[i]);
                        }
                        
                    }
                    
                }
                // return;
                
                // logger.info(`paymentData ====== ${paymentData}`);
                logger.info(` after get finalResponse  ${Date.now()} `);
                // You can define styles as json object
                const styles = {
                    headerDark: {
                        fill: {
                            fgColor: {
                                rgb: 'FF000000'
                            },
                            color: {
                                rgb: 'FFFFFFFF'
                            },
                        },
                        font: {
                            color: {
                                rgb: 'FFFFFFFF'
                            },
                            sz: 14,
                            bold: true,
                            underline: true
                        }
                    }
                };

                //Here you specify the export structure
                const loanSpec = {
                    loanId: { // <- the key should match the actual data key
                        displayName: 'Loan Id', // <- Here you specify the column header
                        headerStyle: styles.headerDark, // <- Header style
                    },
                    mobile: {
                        displayName: 'Mobile',
                        headerStyle: styles.headerDark,
                    },
                    email: {
                        displayName: 'Email',
                        headerStyle: styles.headerDark,
                    },
                    shortUrl: {
                        displayName: 'Attchment',
                        headerStyle: styles.headerDark,
                    },
                    referenceNumber: {
                        displayName: 'Sms Reference Number',
                        headerStyle: styles.headerDark,
                    },
                    smsSend: {
                        displayName: 'SMS Send',
                        headerStyle: styles.headerDark,
                    },
                    emailSent: {
                        displayName: 'Email Send',
                        headerStyle: styles.headerDark,
                    }
                }

                const loandataset = [];
                logger.info(` starting with csv file ${Date.now()}`)
                for (var i = 0; i < legalNoticeSentDataFinal.length; i++) {
                    let data = {};
                    data.loanId = legalNoticeSentDataFinal[i].loanId;
                    data.mobile = legalNoticeSentDataFinal[i].mobile;
                    data.email = legalNoticeSentDataFinal[i].email;
                    data.shortUrl = legalNoticeSentDataFinal[i].shortUrl;
                    data.referenceNumber = legalNoticeSentDataFinal[i].referenceNumber;
                    data.smsSend = legalNoticeSentDataFinal[i].smsSend;
                    data.emailSent = legalNoticeSentDataFinal[i].emailSent;
                    loandataset.push(data);
                }

                logger.info(`after with csv file ${Date.now()}`)

                // Create the excel report.
                // This function will return Buffer
                const report = excel.buildExport([
                    {
                        name: 'PaymentFile', // <- Specify sheet name (optional)
                        specification: loanSpec, // <- Report specification
                        data: loandataset // <-- Report data
                    }
                ]);

                const params = {
                    Bucket: process.env.DATA_IMPORT_BUCKET_NAME,
                    Key: fileName,
                    Body: report,
                };
                logger.info(`params ======= ${params}`);
                const result = await s3.putObject(params).promise();
                logger.info(` result ${result}`);
                body.totalRecord = legalNoticeSentDataFinal.length;
                body.exportTypeName = '';
                await updateIntoDataExport(body, fileUpload.id);
                logger.info(` end ${Date.now()} `)
            }
            else if (fileUpload.operation == 'LEGAL_UPLOAD') {
                //console.log('LEGAL_UPLOAD before for :', rawData);
                let legalAuditData = [];
                let keyWithQoutes = '';
                let arrayOfLoanId = [];
                let totatCount = 0;
                for (var i = 0; i < rawData.length; i++) {
                    arrayOfLoanId.push(rawData[i].loanId);
                    let legalAuditDataTemp = {};
                    keyWithQoutes += `'${rawData[i].loanId}',`;
                    legalAuditDataTemp['loanId'] = rawData[i].loanId;
                    legalAuditDataTemp['mobile'] = rawData[i].mobile;
                    legalAuditDataTemp['email'] = rawData[i].email;
                    //console.log('100 ====')
                    if (!legalAuditDataTemp['mobile'] || !legalAuditDataTemp['email']) {
                        let getContactDataResult = await getContactData(rawData[i].loanId);
                        legalAuditDataTemp['email'] = legalAuditDataTemp['email'] ? legalAuditDataTemp['email'] : getContactDataResult[0].EMAIL;
                        legalAuditDataTemp['mobile'] = legalAuditDataTemp['mobile'] ? legalAuditDataTemp['mobile'] : getContactDataResult[1].contactValue;
                    }
                    //console.log('101 ====')
                    legalAuditDataTemp['createdBy'] = userId;
                    legalAuditDataTemp['updatedBy'] = userId;
                    legalAuditDataTemp['referenceNumber'] = 'U' + userId + 'T' + new Date().getTime() + 'A' + i;

                    let getLoanDetailsDataResult = await getLoanDetailsData(rawData[i].loanId);
                    //console.log('102 ====')
                    let getAddressDataResult = await getAddress(rawData[i].loanId);
                    //console.log('103 ====')
                    legalAuditDataTemp['loanDetails'] = getLoanDetailsDataResult;
                    legalAuditDataTemp['addressDetails'] = getAddressDataResult;
                    //console.log('104 ====')
                    // let getFinancierAddressData = await getFinancierAddress(getLoanDetailsDataResult.financierId);
                    // console.log('105 ====:', getFinancierAddressData)
                    // legalAuditDataTemp['financierData'] = getFinancierAddressData;

                    //console.log('legalAuditDataTemp :', legalAuditDataTemp);

                    //Need to confirm and check
                    //legalAuditDataTemp['referenceNumber'] = (legalAuditDataTemp.loanDetails.financierName).trim() + new Date().getTime() + i;
                    // console.log(" getLoanDetailsDataResult.financierId ", getLoanDetailsDataResult);
                    // return;
                    let stream = await generatePdf(legalAuditDataTemp, getLoanDetailsDataResult);
                    //console.log('106 ====')
                    let objectName = getLoanDetailsDataResult.financierName + "_" + getAddressDataResult.fullName + "_" + rawData[i].loanId + "_" + (legalAuditDataTemp.mobile).substring(legalAuditDataTemp.mobile.length - 4) + ".pdf";
                    objectName = objectName.replace(/\s+/g, '_');
                    let newFolder = (fileUpload.fileName).split('.csv');
                    const params = {
                        Bucket: process.env.LEGAL_NOTICE_BUCKET_NAME + '/' + newFolder[0],
                        Key: objectName,
                        Body: stream,
                    };
                    let shortLinkData = await shortLink(process.env.S3_LEGAL_URL + newFolder[0] + '/' + objectName);
                    const result = await s3.putObject(params).promise();
                    // const result = '';
                    logger.info(` result ${result}`);
                    legalAuditDataTemp['batchNumber'] = fileUpload.id;
                    legalAuditDataTemp['attachment'] = process.env.S3_LEGAL_URL + newFolder[0] + '/' + objectName;
                    legalAuditDataTemp['financierId'] = fileUpload.financierId ? fileUpload.financierId : '';
                    legalAuditDataTemp['shortUrl'] = shortLinkData.status == 'OK' ? shortLinkData.txtly : '';
                    delete legalAuditDataTemp.loanDetails;
                    delete legalAuditDataTemp.addressDetails;
                    //delete legalAuditDataTemp.financierData;//added by saif kamaal and gaus.(19/07/2021)
                    legalAuditData.push(legalAuditDataTemp);
                    logger.info(`legalAuditData.length ${legalAuditData.length}`)
                    if (legalAuditData.length == 50) {
                        // await insertToDB(legalAuditData, 'LEGAL_AUDIT', 'INSERT'); 
                        // totatCount += 50;
                        // legalAuditData = [];
                    }
                }
                await insertToDB(legalAuditData, 'LEGAL_AUDIT', 'INSERT');
            }
            else if (fileUpload.operation == 'SEND_LEGAL_NOTICE') {
                logger.info(" send legal notice ", fileUpload.userId);
                let getUserDatas = await getUserData(fileUpload.userId);
                logger.info(" getUserDatas ", getUserDatas);
                let getBatchNumbersData = rawData;
                let getLookupData = await getLookup();
                logger.info(" getBatchNumbersData ", getBatchNumbersData);
                if (getLookupData.length > 0) {
                    let legalSendInDays = parseInt(getLookupData[0].description);
                    logger.info(`  legalSendInDays ${legalSendInDays}`)
                    for (var i = 0; i < getBatchNumbersData.length; i++) {
                        let batchNumbers = (getBatchNumbersData[i].batchNumber).split(',');
                        loanResponse = await getLegalAuditData(batchNumbers);
                        let smsData = [];
                        let legalAttachment = [];
                        let exceptionHandling = [];
                        // let mobileNumbersArray = ['9022026964','9920936301','8451043905', '8591390865', '9321282790', '6299669283','9022026964','8169317421','8451043905', '8591390865', '9321282790', '6299669283']
                        for (let j = 0; j < loanResponse.length; j++) {
                            let sendLegal = 1;
                            if (getUserDatas[0].role === 'FINANCIER_MANAGER') {
                                let checkLoanIdWithFinancierData = await checkLoanIdWithFinancier(loanResponse[j].loanId, getUserDatas[0].financierId);
                                logger.info(" checkLoanIdWithFinancierData ", checkLoanIdWithFinancierData);
                                logger.info(" checkLoanIdWithFinancierData ", checkLoanIdWithFinancierData.length);
                                if (checkLoanIdWithFinancierData.length > 0) {
                                    sendLegal = 1;
                                } else {
                                    sendLegal = 0;
                                }
                            }
                            if (sendLegal) {
                                logger.info("if sendLegal ");
                                let fullName = loanResponse[j].fullName && camelize((loanResponse[j].fullName).replace(/\s\s+/g, ' ')) || '';
                                let temp = {};
                                temp.smsSend = 'N';
                                temp.emailSend = 'N';
                                temp.loanId = loanResponse[j].loanId;
                                temp.createdBy = getBatchNumbersData[i].userId;
                                temp.updatedBy = getBatchNumbersData[i].userId;
                                temp.batchNumber = loanResponse[j].batchNumber;
                                temp.longRunningProcessId = fileUpload.id;
                                temp.mobile = loanResponse[j].mobile;
                                if (getBatchNumbersData[i].forEmail == 1) {
                                    logger.info("if if sendLegal getBatchNumbersData[i].forEmail == 1 ");
                                    let fileName = (loanResponse[j].attachment).split('/');
                                    temp.attachment = loanResponse[j].attachment;
                                    let emailObject = {
                                        from: process.env.LEGAL_DEFAULT_EMAIL,
                                        to: loanResponse[j].email,// 'saeed.ansari@digiklug.com',// loanResponse[j].email, 
                                        subject: 'Initiation of LEGAL CASE under SEC 420 (Cheating & Dishonesty) & SEC 405 (Criminal breach of trust) of the INDIAN PENAL CODE, 1860',
                                        html: `Dear ${fullName}, <br><br> 
                                        A legal case has been initiated against you on behalf of ${loanResponse[j].loanDetails.nbfcName} under SEC 420 (Cheating & Dishonesty) & SEC 405 (Criminal breach of trust) of the INDIAN PENAL CODE, 1860 due to failure to repayment of loan ${loanResponse[j].loanId}. You have failed to make the payment of your Loan where total outstanding amount is Rs. ${parseFloat(loanResponse[j].loanDetails.totalOutStanding).toFixed(2)}.
                                        <br><br>
                                        Please find the attachment with the details of the legal notice issued on behalf of ${loanResponse[j].loanDetails.nbfcName} by Piyush Pathak.
                                        <br>
                                        You are hereby requested to settle your dues within next 15 days of the receipt of this notice. Upon your failure to settle the said amount, our clients shall be constrained to initiate necessary legal proceedings against you. The legal proceedings shall be at your own risks and responsibility with regards to cost and any consequences of the same.
                                        <br><br>
                                        It is in your best interest to kindly pay Rs. ${parseFloat(loanResponse[j].loanDetails.totalOutStanding).toFixed(2)} to avoid legal proceedings and further consequences thereof.
                                        <br><br>
                                        If you would like to discuss with us, you can call us +91 90294 85813.
                                        <br><br>
                                        Yours faithfully,
                                        <br><br>
                                        Piyush Pathak
                                        <br><br>
                                        Advocate High Court, Acenna for ${loanResponse[j].loanDetails.nbfcName}
                                        <br><br><br><br><br><br>
                                        <span>Disclaimer:</span><br>
                                        <span style="font-size:8px;">The legal notice has been issued upon instruction from ${loanResponse[j].loanDetails.nbfcName} for non-payment of loan dues. Acenna is technology platform which helps banks and financial institution with recovery and collection. The sender holds no liability towards factual matrix pertaining to dues or total outstanding. The content of this email is confidential and intended for the recipient specified in message only. It is strictly forbidden to share any part of this message with any third party, without a written consent of the sender.<span>
                                        `,
                                        emailAttachment: [{ filename: fileName[4], path: loanResponse[j].attachment }]
                                    }
                                    // Checking if we can send the Email
                                    let fetchLastEmailSendDateData = await fetchLastEmailSendDate(loanResponse[j].loanId);
                                    if (fetchLastEmailSendDateData.length > 0) {
                                        let checkifEmaSmsSendData = await checkifEmaSmsSend(fetchLastEmailSendDateData[0].updatedAt);
                                        if (checkifEmaSmsSendData > legalSendInDays) {
                                            let emailResponseCallback = await sendEmail(emailObject);
                                            temp.emailSend = emailResponseCallback.statusCode == 200 ? 'Y' : 'N';
                                        } else {
                                            temp.emailSend = 'N';
                                            let exceptionTemp = {};
                                            exceptionTemp.loanId = loanResponse[j].loanId;
                                            exceptionTemp.userId = fileUpload.userId;
                                            exceptionTemp.operation = 'SEND_LEGAL_NOTICE';
                                            exceptionTemp.exceptionReason = `We can't send legal email again before ${legalSendInDays} days against loandId = ${loanResponse[j].loanId}`;
                                            exceptionTemp.createdBy = fileUpload.userId;
                                            exceptionTemp.updatedBy = fileUpload.userId;
                                            exceptionTemp.batchNumber = fileUpload.id;
                                            exceptionHandling.push(exceptionTemp);
                                        }

                                    } else {
                                        let emailResponseCallback = await sendEmail(emailObject);
                                        temp.emailSend = emailResponseCallback.statusCode == '200' ? 'Y' : 'N';
                                    }

                                }
                                if (getBatchNumbersData[i].forSms == 1) {
                                    // let shortLinkData = await shortLink(loanResponse[j].attachment);
                                    let tempSms = {};
                                    let message = `LEGAL CASE REMINDER Dear ${fullName} failure to re-pay your ${loanResponse[j].loanDetails.nbfc ? loanResponse[j].loanDetails.nbfc : loanResponse[j].loanDetails.financierName} loan is making us take legal actions against you under SEC 420 (Cheating & Dishonesty) & SEC 405 (Criminal Breach of Trust) of the INDIAN PENAL CODE 1860. Click to see your LEGAL NOTICE ${loanResponse[j].shortUrl} PAY NOW Payment Link to STOP Legal Action. TALK to us on 9029485813 to STOP Legal Action Acenna for ${loanResponse[j].loanDetails.nbfc ? loanResponse[j].loanDetails.nbfc : loanResponse[j].loanDetails.financierName}`;
                                    tempSms.sender = process.env.SMS_SENDER_ID;
                                    tempSms.method = 'sms';
                                    tempSms.to = loanResponse[j].mobile;
                                    tempSms.message = message;
                                    let fetchLastSmsSendDateData = await fetchLastSmsSendDate(loanResponse[j].loanId);
                                    logger.info(`fetchLastSmsSendDateData ${JSON.stringify(fetchLastSmsSendDateData)}`)
                                    if (fetchLastSmsSendDateData.length > 0) {
                                        let checkifEmaSmsSendData = await checkifEmaSmsSend(fetchLastSmsSendDateData[0].updatedAt);
                                        logger.info(`checkifEmaSmsSendData ${JSON.stringify(checkifEmaSmsSendData)}`)
                                        if (checkifEmaSmsSendData > legalSendInDays) {
                                            logger.info(`checkifEmaSmsSendData if`)
                                            smsData.push(tempSms);
                                        } else {
                                            let exceptionTemp = {};
                                            exceptionTemp.loanId = loanResponse[j].loanId;
                                            exceptionTemp.userId = fileUpload.userId;
                                            exceptionTemp.operation = 'SEND_LEGAL_NOTICE';
                                            exceptionTemp.exceptionReason = `We can't send legal sms again before ${legalSendInDays} days against loandId = ${loanResponse[j].loanId}`;
                                            exceptionTemp.createdBy = fileUpload.userId;
                                            exceptionTemp.updatedBy = fileUpload.userId;
                                            exceptionTemp.batchNumber = fileUpload.id;
                                            exceptionHandling.push(exceptionTemp);
                                        }
                                    } else {
                                        logger.info(`else inner`)
                                        smsData.push(tempSms);
                                    }

                                }
                                legalAttachment.push(temp);
                            } else {
                                let exceptionTemp = {};
                                exceptionTemp.loanId = loanResponse[j].loanId;
                                exceptionTemp.userId = fileUpload.userId;
                                exceptionTemp.operation = 'SEND_LEGAL_NOTICE';
                                exceptionTemp.exceptionReason = `This financier is not allowed to send legal notice against loandId = ${loanResponse[j].loanId}`;
                                exceptionTemp.createdBy = fileUpload.userId;
                                exceptionTemp.updatedBy = fileUpload.userId;
                                exceptionTemp.batchNumber = fileUpload.id;
                                exceptionHandling.push(exceptionTemp);
                            }
                        }
                        logger.info(` smsData ========= ${smsData} `)
                        let smsResponse = await sendSms(smsData);
                        logger.info(` smsResponse ${JSON.stringify(smsResponse)}`)
                        logger.info(` legalAttachment ${JSON.stringify(legalAttachment)}`)
                        logger.info(` legalAttachment.length ${legalAttachment.length}`)
                        for (var k = 0; k < legalAttachment.length; k++) {
                            if (smsResponse && smsResponse.data) {
                                // logger.info(` smsResponse ======= ${JSON.stringify(smsResponse.data)} `)
                                let checkDuplicateLoanId = smsResponse.data.filter(data => data.mobile == legalAttachment[k].mobile);
                                // logger.info(` checkDuplicateLoanId ${JSON.stringify(checkDuplicateLoanId)}`);
                                if (checkDuplicateLoanId.length > 0) {
                                    legalAttachment[k].smsReferenceId = checkDuplicateLoanId[0].id ? checkDuplicateLoanId[0].id : '';
                                    legalAttachment[k].smsSend = checkDuplicateLoanId[0].id ? 'Y' : 'N';
                                }
                            }
                            delete legalAttachment[k].mobile;

                        }
                        logger.info(` legalAttachment.length legalAttachment ${JSON.stringify(legalAttachment)}`)
                        await insertToDB(legalAttachment, 'LEGAL_AUDIT_DETAILS');
                        await insertToDB(exceptionHandling, 'EXCEPTION_HANDLING');
                    }
                }

            }
            else if (fileUpload.operation == 'LOAN_ALLOCATION') {
                // logger.info(" LOAN_ALLOCATION CALLED ==========  ")
                const loanRawData = rawData;
                if (loanRawData.length) {
                    for (let i = 0; i < loanRawData.length; i++) {
                        loanRawData[i].updatedBy = userId
                    }
                }
                await updateLoan(loanRawData);
            }
            else if (fileUpload.operation == 'USER_UPLOAD') {
                for (let i = 0; i < rawData.length; i++) {
                    rawData[i].createdBy = userId;
                    rawData[i].updatedBy = userId;
                    rawData[i].startDate = rawData[i].startDate.split("/").reverse().join("-");
                    if (rawData[i].endDate)
                        rawData[i].endDate = rawData[i].endDate.split("/").reverse().join("-");
                    else
                        delete rawData[i].endDate;
                    let checkUserData = await checkUser(rawData[i].email);
                    if (checkUserData.length > 0) {
                        rawData[i].userId = checkUserData[0].userId;
                        /* If user is already exist then It update the user with End date, So user will be deactivated */
                        let updateUserData = await updateUser([rawData[i]]);
                        /* If previous user have end date and then the new user will be created */
                        logger.info(" checkUserData[0].endDate ", checkUserData[0].endDate);
                        if (checkUserData[0].endDate) {
                            logger.info(" if ======= ");
                            // delete rawData[i].userId; 
                            // delete rawData[i].endDate; 
                            // let createUserData = await insertToDB([rawData[i]], 'USER');
                            // logger.info(" createUserData === ", createUserData);
                        }
                    } else {
                        delete rawData[i].endDate;
                        rawData[i].isActive = 'Y';
                        rawData[i].isLocked = 'N';
                        let createUserData = await insertToDB([rawData[i]], 'USER');
                        logger.info(" createUserData === ", createUserData);
                    }
                }
            }
            await updateFileUpload('COMPLETED', fileUpload.fileName);
            completedBatchNumber.push(fileUpload.id);
            await sendNotification(fileUpload, 'COMPLETED');
            responseBody = JSON.stringify({
                status: "success",
                message: 'Success',
                data: [],
            });
            logger.info(` ${fileUpload.operation} WITH BATCH NUMBER = ${fileUpload.id} END HERE ${Date.now()} `);
            loggerObject = {
                userId,
                channel,
                endpoint: '',
                feature: feature,
                method: 'GET',
                response: responseBody,
                createdBy: userId,
                updatedBy: userId,
                correlationId: correlationId,
            };
            loggerResponseCallback = await logRequest(loggerObject);
        }
        responseBody = JSON.stringify({
            status: "success",
            message: 'Success',
            data: fileUploadData,
        });
        // res.status(200).json(responseBody);
        // logger.info(`Main result ==== ${JSON.stringify(responseBody)}`);
        return responseBody;
    }
    catch (err) {
        responseBody = JSON.stringify({
            status: "error",
            message: err.message,
            error: err,
        });
        // logger.info(`responseBody catch ==== ${responseBody}`);
        logger.info(`fileUploadData catch ==== ${JSON.stringify(fileUploadData)}`);
        if (typeof fileUploadData !== 'undefined') {
            for (let i = 0; i < fileUploadData.length; i++) {
                if (completedBatchNumber.includes(fileUploadData[i].id)) {
                    //  Marked already completed in loop
                } else {
                    batchNumbers.push(fileUploadData[i].id);
                }
            }
        }

        if (batchNumbers.length > 0) {
            await updateAsFialed(batchNumbers, err.message);
        }
        
        if (typeof fileUploadData !== 'undefined' && batchNumbers.length > 0) {
            await sendNotification(fileUploadData[0], 'FAILED');
        }

        loggerObject = {
            userId,
            channel,
            endpoint: '',
            feature: feature,
            method: 'GET',
            response: responseBody,
            createdBy: userId,
            updatedBy: userId,
            correlationId: correlationId,
        };

        loggerResponseCallback = await logRequest(loggerObject);
        response['status'] = "error";
        response['message'] = err;
        response['data'] = [];
        return response;
        // res.status(400).json(response);
    }
};

const checkUser = async (email) => {
    return new Promise((resolve, reject) => {
        let sql = `SELECT * FROM USER WHERE email = '${email}' order by userId desc limit 1`;
        con.query(sql, [], async (error, result) => {
            if (error) {
                reject(error);
            }
            resolve(result)
        });
    })
}

async function updateUser(objectArray, t) {
    // logger.info(" objectArray ===== ", objectArray);
    return new Promise((resolve, reject) => {
        let sql = '';
        for (let f = 0; f < objectArray.length; f++) {
            let updateEndDate = objectArray[f].endDate ? `endDate = '${objectArray[f].endDate}',` : '';
            // logger.info(" updateUser query ", query);
            sql = sql + `UPDATE USER SET 
                password = '${objectArray[f].password}',
                fullName = '${objectArray[f].fullName}',
                telephone = '${objectArray[f].telephone}', 
                agencyId = '${objectArray[f].agencyId}',
                role = '${objectArray[f].role}', 
                reportingTo = '${objectArray[f].reportingTo}', 
                startDate = '${objectArray[f].startDate}', 
                ${updateEndDate}
                city = '${objectArray[f].city}', 
                state = '${objectArray[f].state}', 
                pincode = '${objectArray[f].pincode}'
                WHERE userId = '${objectArray[f].userId}';`;
        }
        // logger.info(` user update sql ===== ${sql} `);
        // logger.info(` user update sql ===== ${sql}`);
        con.query(sql, [], async function (error, result, fields) {
            if (error) {
                logger.info(error);
                reject(error)
            }
            resolve(result);
        });
    });
}

const getLookup = async () => {
    return new Promise((resolve, reject) => {
        let getFileQuery = `SELECT * FROM LOOKUP WHERE code = 'SEND_LEGAL_INTERVAL_DAYS' `;
        con.query(getFileQuery, [], async (error, result) => {
            if (error) {
                reject(error);
            }
            resolve(result)
        });
    });
};

const checkifEmaSmsSend = async (lastsendDate) => {
    return new Promise((resolve, reject) => {
        var today = new Date();
        var tokenTime = lastsendDate;
        var diffMs = (tokenTime - today); // milliseconds between now & Christmas
        var diffDays = Math.floor(diffMs / 86400000); // days
        var diffHrs = Math.floor((diffMs % 86400000) / 3600000); // hours
        var diffMins = Math.round(((diffMs % 86400000) % 3600000) / 60000); // minutes
        // logger.info(" diffDays ", Math.abs(diffDays))
        resolve(Math.abs(diffDays));
    });
};

const getFileNameFormDb = async () => {
    return new Promise((resolve, reject) => {
        let getFileQuery = `SELECT * FROM LONG_RUNNING_PROCESS WHERE status = 'INITIATED' order by totalRecord`;
        con.query(getFileQuery, [], async (error, result) => {
            if (error) {
                reject(error);
            }
            resolve(result)

        });
    });
};

const updateFileNameAsProcessing = async (ids) => {
    return new Promise((resolve, reject) => {
        let updateQuery = `UPDATE LONG_RUNNING_PROCESS SET status = 'PROCESSING', updatedAt = '${moment(new Date()).format("YYYY-MM-DD HH:mm:ss")}' WHERE id IN(${ids})`;
        con.query(updateQuery, [], async (error, result) => {
            if (error) {
                reject(error);
            }
            resolve(result);
        });
    });
}

const updateAsFialed = async (batchNumber, failedReason) => {
    logger.info(` updateAsFialed called here ${failedReason}`)
    return new Promise((resolve, reject) => {
        let updateQuery = `UPDATE LONG_RUNNING_PROCESS SET status = 'FAILED', failedReason = "${failedReason}", updatedAt = '${moment(new Date()).format("YYYY-MM-DD HH:mm:ss")}' WHERE id IN(${batchNumber})`;
        logger.info(`updateAsFialed updateQuery ${updateQuery}`);
        // logger.info(`updateAsFialed updateQuery ${updateQuery}`);
        con.query(updateQuery, [], async (upError, Upresult) => {
            if (upError) {
                reject(upError);
            }
            resolve(Upresult);
        });
    });
}

async function generatePdf(pdfData, financierData) {
    let fullName = pdfData.addressDetails && pdfData.addressDetails.fullName && camelize((pdfData.addressDetails.fullName).replace(/\s\s+/g, ' ')) || '';
    pdfData.addressDetails.fullName = fullName;
    logger.info(`Invoking generatePdf Function ${pdfData}`);
    return new Promise(async (resolve, reject) => {
        // let doc = new PDFDocument({ size: "A4", margin: 40, password:(pdfData.mobile).substring(pdfData.mobile.length - 4), userPassword:(pdfData.mobile).substring(pdfData.mobile.length - 4) });
        let doc = new PDFDocument({ size: "A4", margin: 40 });
        generateHeader(doc);
        if(financierData.financierId == 1) {
            gerateLiquiloanFirstPage(doc, pdfData);
            generateLiquiloanFooter(doc, financierData.financierName);
            doc.addPage();
            generateHeader(doc);
            gerateLiquiloanSecondPage(doc, pdfData);
            generateLiquiloanFooter(doc, financierData.financierName);
        } 
        else {
            gerateFirstPage(doc, pdfData);
            generateFooter(doc, 1);
            doc.addPage();
            generateHeader(doc);
            gerateSecondPage(doc, pdfData);
            generateFooter(doc, 2);
        }

        doc.end()
        const buffers = []
        doc.on("data", buffers.push.bind(buffers))
        doc.on("end", async () => {
            const pdfData = await Buffer.concat(buffers)
            resolve(pdfData)
        })
    });
}

function camelize(str) {
    // Split the string at all space characters
    return str.split(' ')
        // get rid of any extra spaces using trim
        .map(a => a.trim())
        // Convert first char to upper case for each word
        .map(a => a[0].toUpperCase() + a.substring(1))
        // Join all the strings back together
        .join(" ")
}

function generateHeader(doc) {

    doc
        .image("app/templates/headerleft.png", 50, 4, { width: 100, height: 100 })
        .fillColor("#444444")
        .image("app/templates/headerright.png", 375, 4, { width: 200, height: 100 })
        .fillColor("#444444")
    doc.moveTo(0, 105).lineWidth(2).lineTo(650, 105).fillAndStroke("#142477").moveDown();
}

function gerateFirstPage(doc, pdfData) {
    const toWords = new ToWords();
    let customer_address = `${pdfData.addressDetails ? pdfData.addressDetails.address : ''} ${pdfData.addressDetails.city}, \n${pdfData.addressDetails ? pdfData.addressDetails.state : ''} - ${pdfData.addressDetails.pincode}`;
    let customerInformationTop = 120;
    doc
        .fontSize(10)
        .fillColor('black')
        // .font("Helvetica-Bold")
        .text("By Speed Post/RPAD/Hand Delivery/Email/SMS", 340, customerInformationTop)
        .fontSize(10)
        .font("Helvetica")
        .text("Reference no:", 50, customerInformationTop + 10)
        .text(`${pdfData.referenceNumber}`, 115, customerInformationTop + 10)
        .text("Date:", 50, customerInformationTop + 20)
        .text(`${today}`, 77, customerInformationTop + 20)
        .fontSize(10)
        // .font("Helvetica-Bold")
        .text("To,", 50, customerInformationTop + 49)
        .fontSize(10)
        .font("Helvetica")
        .text(`${pdfData.addressDetails.fullName}`, 50, customerInformationTop + 60)
        .text(`${customer_address}`, 50, customerInformationTop + 70)
        // .text(`${pdfData.addressDetails ? pdfData.addressDetails.address : ''} ${pdfData.addressDetails.city}`, 50, customerInformationTop + 70)
        // .text(`${pdfData.addressDetails ? pdfData.addressDetails.state : ''} - ${pdfData.addressDetails.pincode}`, 50, customerInformationTop + 82)

        .font("Helvetica-Bold")
        .text("For & behalf of:", 50, customerInformationTop + 110, { underline: true })

        .font("Helvetica")
        .text(`${pdfData.loanDetails.nbfcName}.`, 50, customerInformationTop + 122)
        .text(`${pdfData.loanDetails.nbfcAddress}`, 50, customerInformationTop + 132)
        // .text("10A/2, Floor-Ground", 50, customerInformationTop + 132)
        // .text("Plot – 9A/9B, New Sion CHS, Swami Vallabhdas Marg, Road No 24, Sindhi Colony, Sion, ", 50, customerInformationTop + 142)
        // .text("Mumbai 400022", 50, customerInformationTop + 152)

        .font("Helvetica-Bold")
        .text(`Subject: Demand notice on behalf of my client ${pdfData.loanDetails.nbfcName}. Under Section 138 of the Negotiable Instrument Act.`, 50, customerInformationTop + 170, { underline: true })

        .font("Helvetica")
        .text("Dear,", 50, customerInformationTop + 205)
        .fontSize(10)
        .font("Helvetica")
        .text(`I, Piyush Pathak, Advocate, High Court, do hereby serve you with the following demand notice under instructions and on behalf of my client ${pdfData.loanDetails.nbfcName}. (hereinafter referred to as “Client”) having its Registered Office at ${pdfData.loanDetails.nbfcAddress}`, 50, customerInformationTop + 218)
        .moveDown();

    let customeMarginFromTop = 395;
    doc.font("Helvetica-Bold")
        .fontSize(10)
        .text("1.", 60, customeMarginFromTop)
        .font("Helvetica")
        .text(`That Our “Client” is Non-Banking Financial Corporation (NBFC) bearing registration number ${pdfData.loanDetails.nbfcLicense}, given by Registrar of Companies, Mumbai, ${pdfData.loanDetails.nbfcDescription} `, 75, customeMarginFromTop)
        .moveDown();

    customeMarginFromTop += 67;
    doc.font("Helvetica-Bold")
        .fontSize(10)
        .text("2.", 60, customeMarginFromTop)
        .font("Helvetica")
        .text(`That you ${pdfData.addressDetails.fullName} requested to our client for providing Loan Rs.${pdfData.loanDetails.totalOutStanding - pdfData.loanDetails.penalty} {vide agreement dated ${moment(pdfData.loanDetails.startDate).format("DD.MM.YYYY")} (Memorandum of Understanding)} requested for loan (Loan ID ${pdfData.loanId}) and represented, assured and acknowledged via email of the mandate and payment Amount for providing such financial assistance and accordingly loan was provided to you from my client.`, 75, customeMarginFromTop)
        .moveDown();

    customeMarginFromTop += 57;
    doc.font("Helvetica-Bold")
        .fontSize(10)
        .text("3.", 60, customeMarginFromTop)
        .font("Helvetica")
        .text(`That, the loan was provided to you through various checks and internal credit underwriting parameters at the time of loan application and the amount was determined and disbursed to you.`, 75, customeMarginFromTop)
        .moveDown();

    customeMarginFromTop += 32;
    doc.font("Helvetica-Bold")
        .fontSize(10)
        .text("4.", 60, customeMarginFromTop)
        .font("Helvetica")
        .text(`That you are liable to pay Loan amount of `, 75, customeMarginFromTop, { continued: true })
        .moveDown();

    customeMarginFromTop += 0;
    doc.font("Helvetica-Bold")
        .text(`Rs ${pdfData.loanDetails.principalOutStanding + pdfData.loanDetails.penalty} (${toWords.convert(pdfData.loanDetails.principalOutStanding + pdfData.loanDetails.penalty, { currency: true, ignoreDecimal: false })}) `, 75, customeMarginFromTop, { continued: true })

    customeMarginFromTop += 12;
    doc.font("Helvetica")
    doc.text(`to our client. That, you are bound by the terms of the agreement which was executed at the time when you to avail the loan facility from my client. That you ${pdfData.addressDetails.fullName} have failed in making repayments towards loan despite of request & various correspondences which are on record of our client.`, 75, customeMarginFromTop)
        .moveDown();

    customeMarginFromTop += 43;
    doc.font("Helvetica-Bold")
        .fontSize(10)
        .text("5.", 60, customeMarginFromTop)
        .font("Helvetica")
        .text(`Our client has decided to give you final chance by offering you `, 75, customeMarginFromTop, { continued: true })
        .moveDown();

    customeMarginFromTop += 0;
    doc.font("Helvetica-Bold")
        .text(`one – time settlement of Rs ${pdfData.loanDetails.totalOutStanding - pdfData.loanDetails.penalty} by waiving 100% of your dues. `, 75, customeMarginFromTop, { continued: true })

    customeMarginFromTop += 12;
    doc.font("Helvetica")
    doc.text(`The offer is valid till `, 75, customeMarginFromTop, { continued: true })
        .moveDown();

    customeMarginFromTop += 0;
    doc.font("Helvetica-Bold")
        .text(`${afterSomeDate} `, 75, customeMarginFromTop, { continued: true })

    customeMarginFromTop += 0;
    doc.font("Helvetica")
    doc.text(`you can pay using ${pdfData.loanDetails.financierName} app, website or through link which was sent to you on your registered mobile number. You may also call our representative number mentioned at end of this notice for any assistance regarding payment and settlement.`, 75, customeMarginFromTop)
        .moveDown();

}

function gerateLiquiloanFirstPage(doc, pdfData) {
    const toWords = new ToWords();
    let customer_address = `${pdfData.addressDetails ? pdfData.addressDetails.address : ''} ${pdfData.addressDetails.city}, \n${pdfData.addressDetails ? pdfData.addressDetails.state : ''} - ${pdfData.addressDetails.pincode}`;
    let customerInformationTop = 120;
    doc
        .fontSize(10)
        .fillColor('black')
        // .font("Helvetica-Bold")
        .text("By Speed Post/RPAD/Hand Delivery/Email/SMS", 340, customerInformationTop)
        .fontSize(10)
        .font("Helvetica")
        .text("Reference no:", 50, customerInformationTop + 10)
        .text(`${pdfData.referenceNumber}`, 115, customerInformationTop + 10)
        .text("Date:", 50, customerInformationTop + 20)
        .text(`${today}`, 77, customerInformationTop + 20)
        .fontSize(10)
        // .font("Helvetica-Bold")
        .text("To,", 50, customerInformationTop + 49)
        .fontSize(10)
        .font("Helvetica")
        .text(`${pdfData.addressDetails.fullName}`, 50, customerInformationTop + 60)
        .text(`${customer_address}`, 50, customerInformationTop + 70)

        .font("Helvetica-Bold")
        .text("For & behalf of:", 50, customerInformationTop + 110, { underline: true })

        .font("Helvetica")
        .text(`${pdfData.loanDetails.nbfcName}`, 50, customerInformationTop + 122)
        .text(`also known as Liquiloans`, 50, customerInformationTop + 132)
        .text(`${pdfData.loanDetails.nbfcAddress}`, 50, customerInformationTop + 142)

        .font("Helvetica-Bold")
        .text(`Subject: Demand notice on behalf of my client ${pdfData.loanDetails.nbfcName}`, 50, customerInformationTop + 160, { underline: true })

        .font("Helvetica")
        .text("Dear,", 50, customerInformationTop + 180)
        .fontSize(10)
        .font("Helvetica")
        .text(`I, Piyush Pathak, Advocate, High Court, do hereby serve you with the following demand notice under instructions and on behalf of my client ${pdfData.loanDetails.nbfcName}. (hereinafter referred to as “Client”) having its Registered Office at ${pdfData.loanDetails.nbfcAddress}`, 50, customerInformationTop + 190)
        .moveDown();

    let customeMarginFromTop = 350;
    doc.font("Helvetica-Bold")
        .fontSize(10)
        .text("1.", 60, customeMarginFromTop)
        .font("Helvetica")
        .text(`“Client” is M/S.NDX P2P PRIVATE LIMITED also known as (Liquiloans) bearing registration number ${pdfData.loanDetails.nbfcLicense}, given by Registrar of Companies, Mumbai, ${pdfData.loanDetails.nbfcDescription} `, 75, customeMarginFromTop)
        .moveDown();

    customeMarginFromTop += 87;
    doc.font("Helvetica-Bold")
        .fontSize(10)
        .text("2.", 60, customeMarginFromTop)
        .font("Helvetica")
        .text(`That you ${pdfData.addressDetails.fullName} residing at ${customer_address} are a customer of my client.`, 75, customeMarginFromTop)
        .moveDown();
    
    customeMarginFromTop += 57;
        doc.font("Helvetica-Bold")
            .fontSize(10)
            .text("3.", 60, customeMarginFromTop)
            .font("Helvetica")
            .text(`That you ${pdfData.addressDetails.fullName} requested to our client for providing Loan Rs.${pdfData.loanDetails.totalOutStanding - pdfData.loanDetails.penalty} {vide agreement dated ${moment(pdfData.loanDetails.startDate).format("DD.MM.YYYY")} (Memorandum of Understanding)} requested for loan (Loan ID ${pdfData.loanId}) and represented, assured and acknowledged via email of the mandate and payment Amount for providing such financial assistance and accordingly loan was provided to you from my client.`, 75, customeMarginFromTop)
            .moveDown();

    customeMarginFromTop += 57;
    doc.font("Helvetica-Bold")
        .fontSize(10)
        .text("4.", 60, customeMarginFromTop)
        .font("Helvetica")
        .text(`That you ${pdfData.addressDetails.fullName} are liable to pay Outstanding Loan amount of Rs. ${pdfData.loanDetails.totalOutStanding} (${toWords.convert(pdfData.loanDetails.totalOutStanding, { currency: true, ignoreDecimal: false })}) to our client.`, 75, customeMarginFromTop)
        .moveDown();

    customeMarginFromTop += 43;
    doc.font("Helvetica-Bold")
        .fontSize(10)
        .text("5.", 60, customeMarginFromTop)
        .font("Helvetica")
        .text(`That you ${pdfData.addressDetails.fullName} have failed in making such payment despite of request & various correspondences which are on record of our client.`, 75, customeMarginFromTop,)
        .moveDown();

    customeMarginFromTop += 43;
        doc.font("Helvetica-Bold")
            .fontSize(10)
            .text("6.", 60, customeMarginFromTop)
            .font("Helvetica")
            .text(`That looking at the above circumstances, our client may file criminal complaint against you under Sections U/S 420 for cheating and dishonestly inducing delivery of services; U/S 415 Cheating; U/S 506 Punishment for Criminal Intimidation and other Sections under Indian Penal Code, 1860 in case of your failure to make payments. Accordingly, you are hereby called upon to pay to our client the amount of Rs. ${pdfData.loanDetails.totalOutStanding} (${toWords.convert(pdfData.loanDetails.totalOutStanding, { currency: true, ignoreDecimal: false })}) for Invoice raised against you for loan amount which has been provided to you within 15 days of receipt of this notice.`, 75, customeMarginFromTop)
            .moveDown();

    

}

function gerateLiquiloanSecondPage(doc, pdfData) {
    const toWords = new ToWords();
    let customerInformationTop = 115;
    let customeMarginFromTop = 115;

    

    customeMarginFromTop += 50;
    doc.
    font("Helvetica")
    .fontSize(10)
    .fillColor('black')
    .text(`You are also called upon to pay a sum of Rs. 2,000/- against the cost of this legal notice.`, 50, customeMarginFromTop)
        .moveDown();

    customeMarginFromTop += 20;
    doc.text(`In the event of any failure and or neglect to comply with the requirement of this notice, our clients shall be constrained to initiate necessary legal proceedings against you at your own risks and responsibility as to cost and consequences of it.`, 50, customeMarginFromTop)
        .moveDown();

    customeMarginFromTop += 40;
        doc.text(`We reserve all our legal rights and remedies and we are retaining a copy of the present Notice at our office for future reference. Kindly acknowledge the receipt of the present notice.`, 50, customeMarginFromTop)
            .moveDown();

    customeMarginFromTop += 30;
            doc.text(`If you need any further clarification on this, kindly contact us on below.`, 50, customeMarginFromTop)
                .moveDown();

    customeMarginFromTop += 20;
    doc.text(`Email: legal@acenna.in`, 50, customeMarginFromTop)
        .moveDown();

    customeMarginFromTop += 20;
    doc.text(`Phone: +91 96641 45405`, 50, customeMarginFromTop).moveDown();

    customeMarginFromTop += 20;
    doc.image("app/templates/signature.png", 50, customeMarginFromTop, { width: 200 })
        .fillColor("#444444").moveDown();

    customeMarginFromTop += 100;

    doc.font("Helvetica-Bold")
        .fontSize(10)
        .text("Piyush Pathak,", 60, customeMarginFromTop)
        .moveDown();

    customeMarginFromTop += 10;
    doc.font("Helvetica-Bold")
        .fontSize(10)
        .text(`Advocate High Court`, 60, customeMarginFromTop)

        .moveDown();
}

function gerateSecondPage(doc, pdfData) {
    const toWords = new ToWords();
    let customerInformationTop = 125;
    let customeMarginFromTop = 125;

    doc
        .font("Helvetica-Bold")
        .fillColor('black')
        .text("6.", 60, customeMarginFromTop)
        .font("Helvetica")
        .text(`Kindly note that repayment will also make you eligible for further loans with ${pdfData.loanDetails.financierName} which is currently blocked by system due to non – payment of loan amount from your end.`, 75, customeMarginFromTop)
        .moveDown();

    customeMarginFromTop += 30;
    doc
        .font("Helvetica-Bold")
        .fillColor('black')
        .text("7.", 60, customeMarginFromTop)
        .font("Helvetica")
        .text(`That needless to state the timely repayment of the outstanding amount will enhance your bureau score, thereby providing a better opportunity to avail loan facility if needed in the future.`, 75, customeMarginFromTop)
        .moveDown();

    customeMarginFromTop += 30;
    doc.font("Helvetica-Bold")
        .text("8.", 60, customeMarginFromTop)
        .font("Helvetica")
        .text(`Failure to pay the outstanding amount, my client shall initiate suitable legal actions entirely at your own risk, cost & consequences.`, 75, customeMarginFromTop)
        .moveDown();

    customeMarginFromTop += 30
    doc.font("Helvetica-Bold")
        .text("9.", 60, customeMarginFromTop)
        .font("Helvetica")
        .text(`That looking at the above circumstances, our client may file criminal complaint against you under`, 75, customeMarginFromTop)
        .moveDown();

    customeMarginFromTop += 12;
    doc.font("Helvetica-Bold")
        .text(`Sections U/S 420 for cheating and dishonestly inducing delivery of services; U/S 415 Cheating; U/S 506 Punishment for Criminal Intimidation and other Sections under Indian Penal Code, 1860 `, 75, customeMarginFromTop, { underline: true })

    customeMarginFromTop += 25;
    doc.font("Helvetica").text(`in case of your failure to make payments.`, 75, customeMarginFromTop)
        .moveDown();

    customeMarginFromTop += 20
    doc.font("Helvetica-Bold")
        .text("10.", 60, customeMarginFromTop)
        .font("Helvetica")
        .text(`Also, alternatively my client shall be constrained to initiate recovery suit and availing other legal remedies existing under the law against you in a situation of failure at your end.`, 75, customeMarginFromTop)
        .moveDown();

    customeMarginFromTop += 30
    doc.font("Helvetica-Bold")
        .text("11.", 60, customeMarginFromTop)
        .font("Helvetica")
        .text(`Accordingly, you are hereby called upon to pay to our client the amount of `, 75, customeMarginFromTop, { continued: true })
        .moveDown();

    customeMarginFromTop += 0;
    doc.font("Helvetica-Bold")
        .text(`Rs ${pdfData.loanDetails.totalOutStanding - pdfData.loanDetails.penalty} (${toWords.convert(pdfData.loanDetails.totalOutStanding - pdfData.loanDetails.penalty, { currency: true, ignoreDecimal: false })})`, 75, customeMarginFromTop, { continued: true })

    customeMarginFromTop += 12;
    doc.font("Helvetica")
    doc.text(`only for Invoice raised against you for loan amount which has `, 75, customeMarginFromTop, { continued: true })
        .moveDown();

    customeMarginFromTop += 0;
    doc.font("Helvetica-Bold")
        .text(`been provided to you within 15 days of receipt of this notice.`, 75, customeMarginFromTop, { underline: true })

    customeMarginFromTop += 30;
    doc.font("Helvetica-Bold")
        .fontSize(10)
        .text("12.", 60, customeMarginFromTop)
        .font("Helvetica")
        .text(`You are also called upon to pay a sum of Rs.5,000/ against the cost of this legal notice. In the event of any failure and/ or neglect to comply with the requirement of this notice, our clients shall be constrained to initiate necessary legal proceedings against you at your own risks and responsibility as to cost and consequences of it.`, 75, customeMarginFromTop)
        .moveDown();

    customeMarginFromTop += 50;
    doc.text(`We reserve all our legal rights and remedies and we are retaining a copy of the present Notice at our office for future reference. Kindly acknowledge the receipt of the present notice.`, 50, customeMarginFromTop)
        .moveDown();

    customeMarginFromTop += 50;
    doc.text(`If you need any further clarification on this, kindly contact us on below`, 50, customeMarginFromTop)
        .moveDown();

    customeMarginFromTop += 20;
    doc.text(`Email: legal@acenna.in`, 50, customeMarginFromTop)
        .moveDown();

    customeMarginFromTop += 20;
    doc.text(`Phone: +91 96641 45405`, 50, customeMarginFromTop).moveDown();

    customeMarginFromTop += 20;
    doc.image("app/templates/signature.png", 50, customeMarginFromTop, { width: 200 })
        .fillColor("#444444").moveDown();

    customeMarginFromTop += 100;

    doc.font("Helvetica-Bold")
        .fontSize(10)
        .text("Piyush Pathak,", 60, customeMarginFromTop)
        .moveDown();

    customeMarginFromTop += 10;
    doc.font("Helvetica-Bold")
        .fontSize(10)
        .text(`Advocate High Court`, 60, customeMarginFromTop)

        .moveDown();
}

function generateLiquiloanFooter(doc, financierName) {
    doc.moveTo(0, 715).lineWidth(2).lineTo(650, 715).fillAndStroke("#142477")
    doc
        .fontSize(10)
        .fillColor('black')
        .font("Helvetica")
        .text("Delhi, Mumbai, Hyderabad, Bangalore, Ahmedabad, Vizag", 50, 725)
        .moveDown();
    doc
        .fontSize(8)
        .fillColor('black')
        .font("Helvetica")
        .text("Disclaimer:", 50, 740)
        .moveDown();
    doc
        .fontSize(7)
        .fillColor('black')
        .font("Helvetica")
        .text(`The legal notice has been issued upon instruction from ${financierName} for non-payment of loan dues. Acenna is technology platform which helps banks and financial institution with recovery and collection. The sender holds no liability towards factual matrix pertaining to dues or total outstanding. The content of this email is confidential and intended for the recipient specified in message only. It is strictly forbidden to share any part of this message with any third party, without a written consent of the sender.`, 50, 752)
        .moveDown();
    //   doc
    //     .fontSize(10)
    //     .text(
    //         pageNumber,
    //       50,
    //       780,
    //       { align: "center", width: 500 }
    //     );
}

function generateFooter(doc, pageNumber) {
    doc.moveTo(0, 775).lineWidth(2).lineTo(650, 775).fillAndStroke("#142477")
    doc
        .fontSize(10)
        .fillColor('black')
        .font("Helvetica")
        .text("Delhi, Mumbai, Hyderabad, Bangalore, Ahmedabad", 50, 785)
        .moveDown();
    //   doc
    //     .fontSize(10)
    //     .text(
    //         pageNumber,
    //       50,
    //       780,
    //       { align: "center", width: 500 }
    //     );
}

async function logRequest(loggerObject) {
    logger.info("Invoking Logger Function");
    let loggerFunctionName = 'acenna-logger'
    let params = {
        FunctionName: loggerFunctionName,
        InvocationType: "RequestResponse",
        LogType: "Tail",
        Payload: JSON.stringify(loggerObject),
    };
    try {
        const loggerResponse = await lambda.invoke(params).promise();
        // logger.info(` loggerResponse.Payload ${loggerResponse.Payload}`);
        return loggerResponse.Payload;
    }
    catch (loggerError) {
        logger.info(` loggerError ===== ${loggerError}`);
    }
}

async function checkUserStatus(userId) {
    logger.info("Invoking Check USer Status User Function");
    let params = {
        FunctionName: "acenna-user-status",
        InvocationType: "RequestResponse",
        LogType: "Tail",
        Payload: JSON.stringify({ userId }),
    };
    try {
        const callResponse = await lambda.invoke(params).promise();
        return callResponse.Payload;
    }
    catch (error) {
        logger.info("Error in invoking Check User Status Function ", error);
    }
}

async function uploadFileToS3(objectArray) {
    logger.info("Invoking Upload to  Function");
    let params = {
        FunctionName: 'acenna-upload-to-s3',
        InvocationType: "RequestResponse",
        LogType: "Tail",
        Payload: JSON.stringify(objectArray),
    };
    try {
        const loggerResponse = await lambda.invoke(params).promise();
        return loggerResponse.Payload;
    }
    catch (loggerError) {
        logger.info(`Error in invoking Upload to S3 Function = ${loggerError} `);
    }
}

async function fetchLoanId(loanId) {
    let keyWithQoutes = -1;
    if (loanId.length > 0) {
        keyWithQoutes = loanId.map((it) => { return `'${it}'` })
    }


    return new Promise((resolve, reject) => {
        // let sql = `select loanId from LOANEE where loanId = 'DM1002'`;
        let sql = `SELECT loanId FROM LOANEE where loanId IN(${keyWithQoutes})`;
        con.query(sql, [], async function (error, result, fields) {
            if (error) {
                logger.info(error);
                reject(error)
            }
            if (result.length) {
                resolve(result);
            } else {
                resolve(0);
            }
        });
    });
}

async function insertToDB(objectArray, t, INSERT = 'INSERT') {
    // objectArray = JSON.parse(objectArray);
    return new Promise((resolve, reject) => {
        if (objectArray.length > 0) {
            const table = t;
            let keys = Object.keys(objectArray[0]);
            let values = objectArray.map(obj => keys.map(key => obj[key]));

            let sql = '' + INSERT + ' INTO ' + table + ' (' + keys.join(',') + ') VALUES ? ';

            let test = con.query(sql, [values], async function (error, result, fields) {
                if (error) {
                    logger.info(error);
                    reject(error)
                }
                logger.info(`result ${result}`);
                logger.info(`sql ${test.sql}`);
                var rowIds = [];
                for (var i = result.insertId; i < result.insertId + result.affectedRows; i++) {
                    rowIds.push(i);
                }
                resolve(rowIds);
            });
        } else {
            resolve(1)
        }

    });
}

async function getContact(arrayOfLoanId) {
    return new Promise((resolve, reject) => {
        let keyWithQoutes = -1;
        if (arrayOfLoanId.length > 0) {
            keyWithQoutes = arrayOfLoanId.map((it) => { return `'${it}'` })
        }
        const contactSQL = `SELECT * from CONTACT c where c.loanId IN (${keyWithQoutes}) AND isPrimary='Y'`;
        con.query(
            contactSQL, [],
            async (contactError, contactResponse) => {
                if (contactError) {
                    reject(contactError);
                }
                resolve(contactResponse);
            });
    });
}

async function updateLegalAutdit(objectArray) {
    logger.info(" objectArray ", objectArray);
    return new Promise((resolve, reject) => {
        let sql = '';
        for (let i = 0; i < objectArray.length; i++) {
            sql = sql + `UPDATE LOANEE SET agentId = ${objectArray[i].agentId},  updatedBy = ${objectArray[i].updatedBy} where loanId = '${objectArray[i].loanId}';`;
        }
        con.query(sql, [], async function (error, result, fields) {
            if (error) {
                logger.info(error);
                reject(error)
            }
            resolve(result);
        });
    });
}

async function updateContact(objectArray, t) {
    return new Promise((resolve, reject) => {
        // let sql = '';
        // for(let i = 0; i < objectArray.length; i++){
        //     sql =  sql + `UPDATE CONTACT SET agentId = ${objectArray[i].agentId},  updatedBy = ${objectArray[i].updatedBy} where loanId = '${objectArray[i].loanId}';`;
        // }
        let sql = '';
        for (let f = 0; f < objectArray.length; f++) {
            // sql = `UPDATE CONTACT SET isPrimary = 'N', updatedBy = '1001', endDate = '2021-02-01' WHERE 1=1 AND loanId = 'Testing0034' AND endDate IS NULL;`
            sql = sql + `UPDATE CONTACT SET isPrimary='${objectArray[f].isPrimary}', updatedBy='${objectArray[f].updatedBy}', endDate='${objectArray[f].endDate}' WHERE 1=1 AND loanId='${objectArray[f].loanId}' AND endDate IS NULL;`;
        }
        con.query(sql, [], async function (error, result, fields) {
            if (error) {
                logger.info(error);
                reject(error)
            }
            resolve(result);
        });
    });
}

async function updateContactDetails(objectArray, t) {
    return new Promise((resolve, reject) => {
        let sql = '';
        for (let f = 0; f < objectArray.length; f++) {
            if (objectArray[f].contactTypeIsMobile === 'Y') {
                sql = sql + `UPDATE CONTACT_DETAILS SET isPrimary = 'N',
                                        updatedBy = '${objectArray[f].updatedBy}',
                                        endDate = '${objectArray[f].endDate}' 
                                        WHERE contactId = '${objectArray[f].contactId}' AND contactType = 'TELEPHONE' AND endDate IS NULL; `;
            }
            if (objectArray[f].contactTypeIsEmail === 'Y') {
                sql = sql + `UPDATE CONTACT_DETAILS SET isPrimary = 'N',
                                        updatedBy = '${objectArray[f].updatedBy}',
                                        endDate = '${objectArray[f].endDate}' 
                                        WHERE contactId = '${objectArray[f].contactId}' AND contactType = 'EMAIL' AND endDate IS NULL; `;
            }

        }
        if (sql) {
            con.query(sql, [], async function (error, result, fields) {
                if (error) {
                    logger.info(error);
                    reject(error)
                }
                resolve(result);
            });
        } else {
            resolve(1);
        }

    });
}

async function updateAddress(objectArray, t) {
    return new Promise((resolve, reject) => {
        let sql = '';
        for (let f = 0; f < objectArray.length; f++) {
            sql = sql + `UPDATE ADDRESS SET isPrimary = 'N',
                                        updatedBy = '${objectArray[f].updatedBy}',
                                        endDate = '${objectArray[f].endDate}' 
                                        WHERE value = '${objectArray[f].contactId}' AND endDate IS NULL; `;

        }
        con.query(sql, [], async function (error, result, fields) {
            if (error) {
                logger.info(error);
                reject(error)
            }
            resolve(result);
        });
    });
}

async function insertToDB_1(objectArray, t) {
    // objectArray = JSON.parse(objectArray);
    return new Promise((resolve, reject) => {
        const table = t;
        let keys = Object.keys(objectArray[0]);
        let values = objectArray.map(obj => keys.map(key => obj[key]));
        let sql = 'INSERT INTO ' + table + ' (' + keys.join(',') + ') VALUES ?';
        con.query(sql, [values], async function (error, result, fields) {
            if (error) {
                logger.info(error);
                reject(error)
            }
            resolve(result);
        });
    });
}

async function getFile(params) {
    logger.info(" getFile called here ", params);
    return new Promise((resolve, reject) => {
        // var params = {Bucket: 'testuploadacennacollectin', Key: 'LOAN_ALLOCATION_1609743153413_1002.csv'};
        var file = s3.getObject(params).createReadStream();
        const data_arr = [];

        file.pipe(parse())
            .on('error', function (e) { logger.info(`Error in reading file ${e}`); resolve(0) })
            .on('data', function (data) {
                let row = JSON.stringify(data);
                const parsed_data = JSON.parse(JSON.stringify(data));
                data_arr.push(parsed_data);
            })
            .on('end', function () {
                // insert();
                resolve(data_arr)
            });
    });
}

async function updateFileUpload(status, fileName) {
    return new Promise((resolve, reject) => {
        let sql = `UPDATE LONG_RUNNING_PROCESS SET status = '${status}', updatedAt = '${moment(new Date()).format("YYYY-MM-DD HH:mm:ss")}' WHERE fileName = '${fileName}'`;
        con.query(sql, [], async function (error, result, fields) {
            if (error) {
                logger.info(` Error in updateFileUpload ${error}`);
                reject(error);
            }
            resolve(result);
        });
    })
}

async function sendNotification(rowData, status) {
    return new Promise((resolve, reject) => {
        let data = JSON.stringify({ "to": rowData.token, "notification": { "body": "File upload is in progress.", "title": "Processing" } });
        if (status === 'COMPLETED') {
            data = JSON.stringify({ "to": rowData.token, "notification": { "body": "File upload is completed. Please referesh to see the changes.", "title": "Completed" } });
        } else if (status === 'FAILED') {
            data = JSON.stringify({ "to": rowData.token, "notification": { "body": "File uploading failed please contact IT.", "title": "Completed" } });
        }
        var config = {
            method: 'post',
            url: process.env.NOTIFICATION_POST_URL,
            headers: {
                'Content-Type': 'application/json',
                'Authorization': process.env.FIREBASE_SERVER_KEY
            },
            data: data
        };
        axios(config)
            .then(function (response) {
                resolve(1)
            })
            .catch(function (error) {
                resolve(1)
            });
    })
}

async function getLoanDetailsData(loanId) {
    return new Promise((resolve, reject) => {
        const sql = `select ld1.agencyId, ld1.agentId, ld1.financierId,ld1.loanId,ld1.id,ld1.loanAmount,ld1.totalOutStanding,ld1.principalOutStanding, ld1.penalty, ld1.startDate,f.name as financierName,f.nbfcName,f.nbfcLicense,f.nbfcDescription,f.nbfcAddress From LOAN_DEATLS ld1 JOIN FINANCIER f ON f.financierId = ld1.financierId where loanId = '${loanId}' order by startDate desc limit 1`;
        // console.log(" sql ", sql);
        con.query(sql, [], async (sqlError, sqlResponse) => {
            if (sqlError) {
                reject(sqlError);
            }
            if(sqlResponse.length> 0) {
                resolve(sqlResponse[0]);
            } else {
                resolve({
                    agencyId: '',
                    agentId: '',
                    financierId: '',
                    loanId: '',
                    id: '',
                    loanAmount: '',
                    totalOutStanding: '',
                    principalOutStanding: '',
                    startDate: '',
                    penalty: '',
                    nbfcName: '',
                    nbfcLicense: '',
                    nbfcAddress: '',
                    financierName: '',
                    nbfcDescription: ''
                });
            }
            
        });
    });
}

async function getAddress(loanId) {
    return new Promise((resolve, reject) => {
        const sql = `SELECT 
            c.fullName,a.address,a.city,a.state,a.pincode
            FROM
                CONTACT c
                JOIN ADDRESS a ON a.value = c.contactId
            where loanId = '${loanId}' AND a.isPrimary='Y'`;
        con.query(sql, [], async (sqlError, sqlResponse) => {
            if (sqlError) {
                reject(sqlError);
            }
            resolve(sqlResponse[0]);
        });
    });
}

async function getFinancierAddress(financierId) {
    return new Promise((resolve, reject) => {
        const sql = `SELECT * FROM ADDRESS where entity = 'FINANCIER_ID' AND value = '${financierId}' AND isPrimary = 'Y'`;
        con.query(sql, [], async (sqlError, sqlResponse) => {
            if (sqlError) {
                reject(sqlError);
            }
            resolve(sqlResponse[0]);
        });
    });
}

async function getContactData(loanId) {
    return new Promise((resolve, reject) => {
        const sql = `SELECT cd.contactType, cd.contactValue
            FROM
                CONTACT c
                JOIN CONTACT_DETAILS cd ON cd.contactId = c.contactId
            where loanId = '${loanId}' AND c.isPrimary='Y' order by contactType`;
        con.query(sql, [], async (sqlError, sqlResponse) => {
            if (sqlError) {
                reject(sqlError);
            }
            resolve(sqlResponse);
        });
    });
}

async function shortLink(longLink) {
    // http://alerts.kaleyra.com/api/v5/?api_key=A8d9aa5d29d4b57f8f2d9e95b722caf82&method=txtly.create&url=https://uploadlegal.s3.ap-southeast-1.amazonaws.com/LEGAL_UPLOAD_1618640749061_1001/LEGAL_UPLOAD_1618640762159_DMI0006475309.pdf&advanced=1&title=abc&format=json
    // let sms = {"sms": smsData};
    var config = {
        method: 'post',
        url: `${process.env.SMS_BASE_URL1}api_key=${process.env.SMS_API_KEY}&method=txtly.create&url=${longLink}&advanced=1&title=abc&format=js`,
        headers: {
            'Content-Type': 'application/json'
        },
        data: {}
    };
    return new Promise((resolve, reject) => {
        axios(config)
            .then(function (response) {
                resolve(response.data)
            })
            .catch(function (error) {
                resolve(0)
            });
    });
}

async function saveIntoDataExport(body) {
    return new Promise((resolve, reject) => {
        const sql = `INSERT INTO DATA_EXPORT (exportType, exportTypeId, exportTypeName, fileName, status, redirectUrl, token, startDate, endDate, operatingSystem, totalRecord, createdBy, updatedBy, cronJobStatus) VALUES('${body.exportType}', '${body.exportTypeId}', '${body.exportTypeName}', '${body.fileName}', 'COMPLETED', '', '${body.notificationtoken}', '${body.startDate}', '${body.endDate}', 'BROWSER', '${body.totalRecord}', ${body.userId}, ${body.userId}, '1')`;
        con.query(sql, [], async (sqlError, sqlResponse) => {
            if (sqlError) {
                reject(sqlError);
            }
            resolve(sqlResponse);
        });
    });
}

async function updateIntoDataExport(body, id) {
    logger.info(` body ====== ${body}`)
    return new Promise((resolve, reject) => {
        const sql = `UPDATE LONG_RUNNING_PROCESS SET totalRecord = '${body.totalRecord}', status = 'COPLETED', exportFileName = '${body.fileName}', startDate = '${body.startDate}', endDate = '${body.endDate}' where id = ${id}`;
        logger.info(` updateIntoDataExport ======  ${sql}`)
        con.query(sql, [], async (sqlError, sqlResponse) => {
            if (sqlError) {
                reject(sqlError);
            }
            resolve(sqlResponse);
        });
    });
}

async function getUserData(userId) {
    return new Promise((resolve, reject) => {
        const sql = ` Select fullName,role,financierId from USER where userId = ${userId}`;
        con.query(sql, [], async (sqlError, sqlResponse) => {
            if (sqlError) {
                reject(sqlError);
            }
            resolve(sqlResponse);
        });
    });
}

async function getAgencyFinancer(table, id = 0) {
    let where = 'WHERE 1=1';

    return new Promise((resolve, reject) => {
        let idName = table == "AGENCY" ? "agencyId" : "financierId";
        if (id > 0) {
            where += ` AND ${idName} = ${id}`;
        }
        const sql = ` Select name, ${idName} id from ${table} ${where}`;
        con.query(sql, [], async (sqlError, sqlResponse) => {
            if (sqlError) {
                reject(sqlError);
            }
            resolve(sqlResponse);
        });
    });
}

async function getExpOrtdataCount(body) {
    let whereCondition = ` where 1=1 AND startDate between '${body.startDate}' AND '${body.endDate}'`;
    if (body.exportType === 'FINANCIER') {
        if (body.exportTypeId != 0) {
            whereCondition += ` AND l.financierId = ${body.exportTypeId}`;
        } else {
            whereCondition += ` AND l.financierId <> 0`;
        }
    }
    else if (body.exportType === 'AGENCY') {
        if (body.exportTypeId != 0) {
            whereCondition += ` AND l.agencyId = ${body.exportTypeId}`;
        } else {
            whereCondition += ` AND l.agencyId <> 0`;
        }

    }
    else if (body.exportType === 'USER') {
        if (body.exportTypeId != 0) {
            whereCondition += ` AND l.createdBy = ${body.exportTypeId}`;
        } else {
            whereCondition += ` AND l.createdBy <> 0`;
        }
    }

    let FJOIN = ``;
    if(body.downloadFileName == 'PAYMENT_FILE') {
        FJOIN += `JOIN LOAN_TRANSACTION lt ON lt.loanId = ld.loanId`;
        whereCondition += ` AND (lt.dispositionCode = 'PAID' OR lt.dispositionCode = 'PARTIAL PAID')`;
    }

    return new Promise((resolve, reject) => {
        let loanSQL = ``;
        if(body.downloadFileName == 'LEGAL_NOTICE') {
            loanSQL = `SELECT 
            count(1) as count
            FROM LEGAL_AUDIT la 
            JOIN LOAN_DEATLS ld ON ld.loanId = la.loanId
            where 1=1 AND la.createdAt between '${body.startDate}' AND '${body.endDate}' AND la.financierId = '${body.exportTypeId}'`;
        } 
        else {
            loanSQL = `SELECT 
              count(1) as count
                FROM
                    LOANEE l JOIN LOAN_DEATLS ld ON ld.loanId = l.loanId
                    ${FJOIN}
                    ${whereCondition}`;
        }
        

        logger.info(`getExpOrtdata ======== ${loanSQL}`)

        con.query(loanSQL, [], async (loanError, loanResponse) => {
            if (loanError) {
                reject(loanError);
            }
            resolve(loanResponse);
        });
    });


}

async function getExpOrtdata(body, offset, limit) {
    let whereCondition = ` where 1=1 AND startDate between '${body.startDate}' AND '${body.endDate}'`;
    if (body.exportType === 'FINANCIER') {
        if (body.exportTypeId != 0) {
            whereCondition += ` AND l.financierId = ${body.exportTypeId}`;
        } else {
            whereCondition += ` AND l.financierId <> 0`;
        }
    }
    else if (body.exportType === 'AGENCY') {
        if (body.exportTypeId != 0) {
            whereCondition += ` AND l.agencyId = ${body.exportTypeId}`;
        } else {
            whereCondition += ` AND l.agencyId <> 0`;
        }

    }
    else if (body.exportType === 'USER') {
        if (body.exportTypeId != 0) {
            whereCondition += ` AND l.createdBy = ${body.exportTypeId}`;
        } else {
            whereCondition += ` AND l.createdBy <> 0`;
        }
    }
    let FJOIN = ``;
    if(body.downloadFileName == 'PAYMENT_FILE') {
        FJOIN += `JOIN LOAN_TRANSACTION lt ON lt.loanId = ld.loanId`;
        whereCondition += ` AND (lt.dispositionCode = 'PAID' OR lt.dispositionCode = 'PARTIAL PAID')`;
    } 
    let limits = ``;
    if (limit) {
        limits = ` LIMIT ${limit} OFFSET ${offset}`;
    }
    return new Promise((resolve, reject) => {
        let loanSQL = ``;
        if(body.downloadFileName == 'LEGAL_NOTICE') {
            loanSQL = `SELECT 
                l.loanId,
                l.referenceLoanId,
                l.loanType,
                l.agencyAllocationDate,
                l.allocationDPD,
                l.loanAmount,
                l.principalOutStanding emi,
                l.penalty due,
                l.totalOutStanding outstanding,
                l.category,
                l.mandateStatus,
                l.emiPaymentDate,
                l.maturityDate,
                l.loanTenure,
                l.agentId,
                l.agencyId,
                l.financierId
            FROM LEGAL_AUDIT la
                JOIN LOAN_DEATLS ld ON ld.loanId = la.loanId
                JOIN LOANEE l ON l.loanId = la.loanId
            WHERE 1 = 1
                AND la.createdAt BETWEEN '${body.startDate}' AND '${body.endDate}'
                AND la.financierId = '${body.exportTypeId}'
                ${limits}`;
        }
        else 
        {
            loanSQL = `SELECT 
              l.loanId, l.referenceLoanId, l.loanType, l.agencyAllocationDate, l.allocationDPD, l.loanAmount, l.principalOutStanding emi, l.penalty due, l.totalOutStanding outstanding, l.category, l.mandateStatus, l.emiPaymentDate, l.maturityDate, l.loanTenure, 
              l.agentId, l.agencyId, l.financierId
                FROM
                    LOANEE l JOIN LOAN_DEATLS ld ON ld.loanId = l.loanId 
                    ${FJOIN}
                    ${whereCondition} ${limits}`;
        }
        

        logger.info(`getExpOrtdata ========= ${loanSQL}`);
        // return;

        con.query(loanSQL, [], async (loanError, response) => {
            if (loanError) {
                reject(loanError);
            }
            resolve(response);
        });
    });


}

async function getContactDetail(arrayOfContactId) {
    logger.info("Invoking Get Contacts Details Function");
    return new Promise((resolve, reject) => {
        if (arrayOfContactId.length <= 0) {
            arrayOfContactId = [-1];
        }
        const contactSQL =
            "SELECT d.contactDetailId, d.contactId, d.contactType, d.contactValue, d.isPrimary, d.isActive from CONTACT_DETAILS d where d.contactId IN (" +
            arrayOfContactId.join() +
            ")";
        con.query(contactSQL, [], async (contactError, contactResponse) => {
            if (contactError) {
                reject(contactError);
            }
            resolve(contactResponse);
        });
    });
}

async function getContactAddress(arrayOfContactId) {
    logger.info("Invoking Get Contacts Address Function");
    return new Promise((resolve, reject) => {
        if (arrayOfContactId.length <= 0) {
            arrayOfContactId = [-1];
        }
        const contactAddressSQL =
            "SELECT a.addressId, a.entity, a.value, a.address, a.addressType, a.isPrimary, a.latitude, a.longitude, a.landmark, a.line1, a.line2, a.roomNumber, a.city, a.district, a.taluka, a.state, a.division, a.pincode from ADDRESS a where a.entity = 'CONTACT_ID' and a.value in (" +
            arrayOfContactId.join() +
            ")";
        con.query(
            contactAddressSQL, [],
            async (contactAddressError, contactAddressResponse) => {
                if (contactAddressError) {
                    reject(contactAddressError);
                }
                resolve(contactAddressResponse);
            }
        );
    });
}

async function getLoanDetails(arrayOfLoanId, downloadFileName) {
    logger.info("Invoking Get Loan Details Function");
    return new Promise((resolve, reject) => {
        let keyWithQoutes = -1;
        if (arrayOfLoanId.length > 0) {
            keyWithQoutes = arrayOfLoanId.map((it) => {
                return `'${it}'`;
            });
        }

        let whereCondition = `WHERE 1=1 AND lt.loanId in  (${keyWithQoutes})`;
        if(downloadFileName == 'LEGAL_NOTICE') {
            
        }
        else if(downloadFileName == 'PAYMENT_FILE') {
            whereCondition += ` AND dispositionCode = 'PAID' OR dispositionCode = 'PARTIAL PAID'`;
        }
        //let keyWithQoutes = arrayOfLoanId.map((it) => { return `'${it}'` })
        const loanDetailSQL = `SELECT * from LOAN_TRANSACTION lt ${whereCondition} `;
        con.query(
            loanDetailSQL, [],
            async (loanDetailError, loanDetailResponse) => {
                if (loanDetailError) {
                    reject(loanDetailError);
                }
                resolve(loanDetailResponse);
            }
        );
    });
}

async function sendEmail(emailObject) {
    let params = {
        FunctionName: "acenna-email",
        InvocationType: "RequestResponse",
        LogType: "Tail",
        Payload: JSON.stringify(emailObject),
    };
    try {
        const emailResponse = await lambda.invoke(params).promise();
        // logger.info(`Email Response: ${emailResponse.Payload}`);
        return JSON.parse(emailResponse.Payload);
    }
    catch (emailError) {
        logger.info(`Error in invoking Email Function: ${emailError}`);
    }
}

async function sendSms(smsData) {
    let sms = { "sms": smsData };
    var config = {
        method: 'post',
        url: `${process.env.SMS_BASE_URL2}api_key=${process.env.SMS_API_KEY}&method=sms.json`,
        headers: {
            'Content-Type': 'application/json'
        },
        data: sms
    };
    logger.info(`sms config: ${JSON.stringify(config)}`);
    return new Promise((resolve, reject) => {
        axios(config)
            .then(function (response) {
                logger.info(`response: ${response.data}`);
                resolve(response.data)
            })
            .catch(function (error) {
                logger.info(` Error in sending sms ========== ${error} `);
                resolve(0)
            });
    });
}

async function getLegalAuditData(arrayOfBatch) {
    logger.info("Invoking getLegalAuditData Function");
    return new Promise((resolve, reject) => {
        let keyWithQoutes = -1;
        if (arrayOfBatch.length > 0) {
            keyWithQoutes = arrayOfBatch.map((it) => {
                return `'${it}'`;
            });
        }
        //let keyWithQoutes = arrayOfLoanId.map((it) => { return `'${it}'` })
        const loanDetailSQL = `SELECT id,batchNumber,attachment,mobile,email,la.loanId,c.fullName,la.shortUrl from LEGAL_AUDIT la JOIN CONTACT c ON c.loanId = la.loanId WHERE la.batchNumber in (${keyWithQoutes}) AND c.isPrimary = 'Y'`;
        con.query(
            loanDetailSQL, [],
            async (loanDetailError, loanDetailResponse) => {
                if (loanDetailError) {
                    reject(loanDetailError);
                }
                if (loanDetailResponse.length > 0) {
                    for (var i = 0; i < loanDetailResponse.length; i++) {
                        loanDetailResponse[i]['loanDetails'] = await getLoanDetailsData(loanDetailResponse[i].loanId);
                    }
                    resolve(loanDetailResponse);
                } else {
                    resolve(0);
                }
            }
        );
    });
}

async function updateLoan(objectArray) {
    logger.info("Invoking Update Records to DB Function: ", objectArray);
    return new Promise((resolve, reject) => {
        let sql = '';
        for (let i = 0; i < objectArray.length; i++) {
            sql = sql + `UPDATE LOANEE SET agentId = ${objectArray[i].agentId}, remarks = CONCAT(COALESCE(remarks,''),' ${moment(new Date()).format("YYYY-MM-DD HH:mm:ss")} : ',' ${objectArray[i].remarks} ', ','), updatedBy = ${objectArray[i].updatedBy}, agencyId = ${objectArray[i].agencyId} where loanId = '${objectArray[i].loanId}';`;
        }
        // logger.info(" sql ========== ", sql)
        con.query(sql, [], async function (error, result, fields) {
            if (error) {
                logger.info(error);
                reject(error)
            }
            logger.info(result);
            resolve(result);
        });
    });
}

async function fetchLastSmsSendDate(loanId) {
    // logger.info("fetchLastSmsSendDate: ", loanId);
    return new Promise((resolve, reject) => {
        // logger.info(" sql ========== ", sql)
        let sql = `select loanId,createdAt,updatedAt,smsSend,emailSend from LEGAL_AUDIT_DETAILS where loanId = '${loanId}' AND smsSend = 'Y' order by updatedAt desc limit 1`;
        con.query(sql, [], async function (error, result, fields) {
            if (error) {
                logger.info(error);
                reject(error)
            }
            // logger.info(result);
            resolve(result);
        });
    });
}

async function fetchLastEmailSendDate(loanId) {
    // logger.info("fetchLastEmailSendDate: ", loanId);
    return new Promise((resolve, reject) => {

        let sql = `select loanId,createdAt,updatedAt,smsSend,emailSend from LEGAL_AUDIT_DETAILS where loanId = '${loanId}' AND emailSend = 'Y' order by updatedAt desc limit 1`;
        // logger.info(" sql ========== ", sql)
        con.query(sql, [], async function (error, result, fields) {
            if (error) {
                logger.info(error);
                reject(error)
            }
            // logger.info(result);
            resolve(result);
        });
    });
}

async function checkLoanIdWithFinancier(loanId, financierId) {
    // logger.info("fetchLastEmailSendDate: ", loanId);
    return new Promise((resolve, reject) => {

        let sql = `SELECT * FROM LOAN_DEATLS WHERE loanId = '${loanId}' AND financierId = '${financierId}' limit 1`;
        // logger.info(" sql ========== ", sql)
        con.query(sql, [], async function (error, result, fields) {
            if (error) {
                logger.info(error);
                reject(error)
            }
            // logger.info(result);
            resolve(result);
        });
    });
}

async function getLoanData(loanId, loanAmount, month) {
    return new Promise((resolve, reject) => {
        let sql = `SELECT id,loanId,dispositionCode,dispositionDate,totalAmount FROM LOAN_TRANSACTION WHERE loanId = '${loanId}' AND totalAmount = '${loanAmount}' AND month(dispositionDate) = month('${month}') limit 1`;
        logger.info(" sql ", sql);
        con.query(sql, [], async function (error, result, fields) {
            if (error) {
                logger.info(error);
                reject(error)
            }
            logger.info(" result ", result);
            resolve(result);
        });
    });
}

async function getPaymentData(body) {
    return new Promise((resolve, reject) => {
        let paymentSql = `SELECT 
        lt.agencyId,
        lt.agentId,
        lt.loanId,
        lt.totalAmount as paymentAmount,
        lt.paymentMode,
        lt.createdAt as paymentDateTime,
        a.name as agencyName,
        c.fullName as customerName
        #(select f.name from LOAN_DEATLS ld JOIN FINANCIER f ON f.financierId = ld.financierId where loanId = lt.loanId order by ld.createdAt desc limit 1) as financierName
        FROM
        LOAN_TRANSACTION lt
        JOIN AGENCY a ON a.agencyId = lt.agencyId
        LEFT JOIN CONTACT c ON c.loanId = lt.loanId
        WHERE 1=1 AND c.isPrimary = 'Y' AND lt.dispositionCode = 'PAID' AND lt.createdAt between '${body.startDate}' AND '${body.endDate}'`;
        // console.log(" paymentSql ===== ", paymentSql);
        con.query(paymentSql, [], async function (error, result, fields) {
            if (error) {
                logger.info(error);
                reject(error)
            }
            logger.info(" result ", result);
            resolve(result);
        });
    })
}

async function getLegalNoticeData(body) {
    return new Promise((resolve, reject) => {
        let legalNoticeSql = `SELECT 
        la.loanId,
        la.mobile,
        la.email,
        la.attachment,
        la.financierId,
        la.shortUrl,
        la.referenceNumber,
        la.emailSent,
        la.smsSend
        FROM
        LEGAL_AUDIT la
        WHERE 1=1 AND la.createdAt between '${body.startDate}' AND '${body.endDate}'`;
        // console.log(" legalNoticeSql ===== ", legalNoticeSql);
        con.query(legalNoticeSql, [], async function (error, result, fields) {
            if (error) {
                logger.info(error);
                reject(error)
            }
            // logger.info(" result ", result);
            resolve(result);
        });
    })
}