var cron = require('node-cron');

cron.schedule('1 * * * *', () => {
    var request = require('request');
    var options = {
      'method': 'GET',
      'url': `http://localhost:${process.env.PORT}/${process.env.ENVIRONMENT_MODE}/api/long-process`,
      'headers': {
      }
    };
    request(options, function (error, response) {
      if (error) throw new Error(error);
      console.log(response.body);
    });
});