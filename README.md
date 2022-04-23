# README #

This README would normally document whatever steps are necessary to get your application up and running.

### How do I get set up? ###

* Run below mentioned command to start running the APIs:
$ pm2 start server.js --name smartBDE_API --watch

* Configuration:
Add configuration in nginx conf file, save it & then run restart nginx by running below mentioned command:
$ taskkill /f /IM nginx.exe
$ start nginx
