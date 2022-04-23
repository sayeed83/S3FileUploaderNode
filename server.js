require('dotenv-safe').config();
var express = require('express');
var app = express();
const bodyParser = require('body-parser');
const cors = require('cors');
const i18n = require('i18n');


// Setup express server port from ENV, default: 3000
app.set('port', process.env.PORT || 3000);

// for parsing json
app.use(
    bodyParser.json({
        limit: '20mb'
    })
)
// for parsing application/x-www-form-urlencoded
app.use(
    bodyParser.urlencoded({
        limit: '20mb',
        extended: true
    })
);

// i18n
i18n.configure({
    locales: ['en', 'de'],
    directory: `${__dirname}/locales`,
    defaultLocale: 'en',
    objectNotation: true
})
app.use(i18n.init)

app.use(cors());
// app.use('/', require('./app/routes/auth'));
app.use(`/api`, require('./app/routes/api'));

app.listen(app.get('port'));


