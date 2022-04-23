const express = require('express');
// var cron = require('node-cron');
const axios = require('axios');
const schedule = require('node-schedule');
const router = express.Router();
const presignedUrl = require('../controllers/presigneUrl.js');
const reads3Datas = require('../controllers/reads3Data.js');
router.get('/presigned-url/:fileName', presignedUrl.presigneUrl);
router.get('/read-s3-data/:fileName', reads3Datas.reads3Data);
module.exports = router;
