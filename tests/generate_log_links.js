/* eslint-disable */
const timer = require('timers/promises');
const md5 = require('md5');
const fluxAPI = require('../lib/fluxAPI');
const log = require('../lib/log');

async function generateLinks() {

    appList = await fluxAPI.getAllApps();
    for (let i = 0; i < appList.length; i++) {

    }

}

//testFluxAPI();
//testFluxAPI2();
testFluxAPI3();

