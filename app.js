'use strict';
const pg = require('pg');
const fs = require('fs');
const _ = require('lodash');
const Pool = pg.Pool;
const MAX_CLIENTS = 20;

let pool;
let argv = {};
_.forEach(process.argv, (arg) => {
    if(arg.split('=').length < 2){
        return;
    }
    argv[arg.split('=')[0]] = arg.split('=')[1];
});

let doExit = false;
const requiredArgv = ['host', 'user', 'database', 'port', 'password'];
_.forEach(requiredArgv, (env) => {
    if(!argv[env] && argv[env] !== ''){
        console.log(env + ' must be set');
        doExit = true;
    }
});

const dbHost = argv.host;
const dbUser = argv.user;
const dbDB = argv.database;
const dbPort = argv.port;
const dbPassword = argv.password;
//postgresql://dbuser:secretpassword@database.server.com:3211/mydb
const connectionString = 'postgresql://' + dbUser + ':' + dbPassword + '@' + dbHost + ':' + dbPort + '/' + dbDB;

async function go(){
    await initDb();
    /*let airports = (await getAirports()).rows;
    airports = _.map(airports, (r) => r.airport.substr(1))
    console.log(airports);*/
    const airports = parseCSV();
    _.forEach(airports, async (airport) => {
        //console.log('inserting: ', airport, ':::', airport.code, ', ', airport.longitude, ', ', airport.latitude);
        try{
            await insertAirport(airport.code, airport.longitude, airport.latitude);    
        }catch(e){
            console.log('Error inserting: ' + airport.code + ', ' + airport.longitude + ', ' + airport.latitude);
        }
        
    });
}

function parseCSV(){
    let content = fs.readFileSync('./airports.dat', 'utf8');
    content = content.split('\n').slice(0, -1);
    let airports = _.map(content, (row) => {
        const entries = row.split(',');
        /*console.log('entries: ', entries);
        if(!entries || entries.length < 8){
            return {};
        }*/
        let code = entries[4].substr(1, entries[4].length - 2);
        let longitude = parseInt(entries[6], 10);//.substr(1, entries[6].length - 2);
        let latitude = parseInt(entries[7], 10);//M.substr(1, entries[7].length - 2);    
        if(!_.isNumber(longitude) || _.isNaN(longitude)){
            console.log('longitude is not number: ', longitude);
            code = entries[5].substr(1, entries[5].length - 2);
            longitude = parseInt(entries[7], 10);//.substr(1, entries[6].length - 2);
            latitude = parseInt(entries[8], 10);//M.substr(1, entries[7].length - 2);
        }
        return {code: code, longitude: longitude, latitude: latitude};
    });

    let airportObj = {};
    _.forEach(airports, (airport) => {
        airportObj[airport.code] = airport;
    });

    return airportObj;
}

go().then(() => {
    console.log('Done!');
}).catch((err) => console.log(err));

async function getAirports(){
    return pool.query('select * from (select distinct schd_from as airport from flights union select distinct schd_to as airport from flights union select distinct real_to as airport from flights ) t0;');
} 

async function insertAirport(airport, longitude, latitude){
    console.log('insert: ' + airport + ', ' + latitude + ', ' + longitude);
    await pool.query('INSERT INTO airports(airport,latitude,longitude) VALUES ($1,$2,$3);', [airport, latitude, longitude]);
}

async function initDb(){
    console.log('Connecting to: ' + connectionString);
    pool = new Pool({
        connectionString: connectionString,
        max: MAX_CLIENTS
    });
    await pool.query('DROP TABLE IF EXISTS airports;');
    await pool.query('CREATE TABLE IF NOT EXISTS airports (airport VARCHAR(10) PRIMARY KEY, latitude real, longitude real);');
}
