const express = require("express");
const config = require('../config.js');

const router = express.Router();

router.get("/", function (req, res) {
    res.send("Search / Twitter API toolkit for AWS");
});

module.exports = router
