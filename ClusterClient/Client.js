const net = require('net');
const mySQLServer = require('../lib/mysqlServer');
const mySQLConsts = require('../lib/mysqlConstants');

class Client{

    static handleAuthorize(param) {
        console.log('Auth Info:');
        console.log(param);
        // Yup you are authorized
        return true;
    }

    static async handleCommand({ command, extra }) {
        // command is a numeric ID, extra is a Buffer
        switch (command) {
            case mySQLConsts.COM_QUERY:
            const query = extra.toString(); 
            console.log(`Got Query: ${query}`);
            //forward the query to the server
            var result = await this.localDB.query(query,true);
            console.log(result);
            // Then send it back to the user in table format
            if(result[1]){
                let fieldNames = [];
                for (let definition of result[1]) fieldNames.push(definition.name);
                this.sendDefinitions(result[1]);
                let finalResult = [];
                for (let row of result[0]){
                let newRow =[];
                for(let filed of fieldNames){
                    newRow.push(row[filed]);
                }
                finalResult.push(newRow);
                }
                //console.log(finalResult);
                this.sendRows(finalResult);
            } else{
                this.sendRequestHeader({ message: '' });
            }
            
            break;
            case mySQLConsts.COM_PING:
            this.sendOK({ message: 'OK' });
            break;
            case null:
            case undefined:
            case mySQLConsts.COM_QUIT:
            console.log('Disconnecting');
            this.end();
            break;
            case mySQLConsts.COM_INIT_DB:
            var result = await this.localDB.query(`use ${extra}`);
            console.log(`extra is ${extra}`)
            this.sendOK({ message: 'OK' });
            break;
            default:
            console.log(`Unknown Command: ${command}`);
            this.sendError({ message: 'Unknown Command' });
            break;
        }
    }
    /**
     * [initDBProxy]
     * @param {string} serverType [description]
     */
    static initProxyDB(serverType) {
        if(serverType==='mysql'){
        //init mysql port
        net.createServer((so) => {
            const server = mySQLServer.createServer({
            socket: so,
            onAuthorize: this.handleAuthorize,
            onCommand: this.handleCommand,
            localDB: this.localDB,
            });
        }).listen(config.externalDBPort);
        
        console.log(`Started proxy mysql server on port ${config.externalDBPort}`);
        }
    } 

}

module.exports = Client;