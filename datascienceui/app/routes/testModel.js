var express = require('express');
var router = express.Router();
var unirest = require('unirest');

/* GET home page. */
router.get('/', function(req, res, next) {
  res.render('testModel', { title: 'Cognitive Claims'});
});

module.exports = router;