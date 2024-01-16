const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const dfd = require('danfojs-node');
const struct = require('python-struct');


class VWAPCalculator {
    constructor() {
        this.temp = [];
        this.flag = null;

        // Creating the 'output' directory if it doesn't exist
        const outputPath = path.join('.', 'output');
        if (!fs.existsSync(outputPath)) {
            fs.mkdirSync(outputPath);
        }
    }

    getBinary(size) {
        // Assuming binData is a readable stream, replace it with your own data source
        const read = binData.read(size);
        return read;
    }

    convertTime(stamp) {
        const time = new Date(stamp / 1e6);
        const formattedTime = time.toISOString().substr(11, 8);
        return formattedTime;
    }

    calVWAP(df) {
        df.addColumn({ colname: 'amount', value: df['price'].multiply(df['volume']) });
        df['time'] = df['time'].to_datetime();

        // Grouping by hour and symbol, summing 'amount' and 'volume'
        df = df.groupby([df['time'].dt.hour, df['symbol']])['amount', 'volume'].sum();

        // Calculating VWAP
        df.addColumn({ colname: 'vwap', value: df['amount'].divide(df['volume']).round(2) });
        df.reset_index({ drop: true, inplace: true });

        // Formatting time
        df['time'] = df.apply(row => row['time'] + ':00:00', { axis: 1 });

        // Selecting required columns
        df = df.loc({ columns: ['time', 'symbol', 'vwap'] });
        return df;
    }

    getVWAP(message) {
        const { parsedData, hour } = this.tradeMessage(message);

        if (this.flag === null) {
            this.flag = hour;
        }

        if (this.flag !== hour) {
            const df = new dfd.DataFrame(this.temp, { columns: ['time', 'symbol', 'price', 'volume'] });
            const result = this.calVWAP(df);

            // Saving result to a file
            result.to_csv(`../output/${this.flag}.txt`, { sep: ' ', index: false });

            // Logging result
            console.log(result.toString());

            this.temp = [];
            this.flag = hour;
        }

        this.temp.push(parsedData);
    }

    tradeMessage(msg) {
        const msgType = 'P';
        const temp = struct.unpack('>4s6sQcI8cIQ', msg);
        console.log(temp)
        const newMsg = struct.pack('>s4s2s6sQsI8sIQ', msgType, temp[0], '\x00\x00', temp[1], temp[2], temp[3], temp[4],
            temp.slice(5, 13).join(''), temp[13], temp[14]);
        let value = struct.unpack('>sHHQQsI8sIQ', newMsg);
        console.log(newMsg)
        value = [...value];
        value[3] = this.convertTime(value[3]);
        value[7] = value[7].trim();
        value[8] = parseFloat(value[8]);
        value[8] /= 10000;
        return [value[3], value[7], value[8], value[6]], value[3].split(':')[0];
    }

}



// Replace with the path to your data file
const filePath = path.join(__dirname, 'tick-data-file', '01302019.NASDAQ_ITCH50.gz');

// Create a readable stream from the gzipped file
const binData = fs.createReadStream(filePath).pipe(zlib.createGunzip());

const itch = new VWAPCalculator();

binData.on('readable', () => {
    let msgHeader;
    while ((msgHeader = binData.read(1)) !== null) {
        let message;

        switch (msgHeader.toString()) {
            case 'S':
                message = itch.getBinary(11);
                // Handle message of type S
                break;

            case 'R':
                message = itch.getBinary(38);
                // Handle message of type R
                break;

            case 'H':
                message = itch.getBinary(24);
                // Handle message of type H
                break;

            case 'Y':
                message = itch.getBinary(19);
                // Handle message of type Y
                break;

            case 'L':
                message = itch.getBinary(25);
                // Handle message of type L
                break;

            case 'V':
                message = itch.getBinary(34);
                // Handle message of type V
                break;

            case 'W':
                message = itch.getBinary(11);
                // Handle message of type W
                break;

            case 'K':
                message = itch.getBinary(27);
                // Handle message of type K
                break;

            case 'h':
                message = itch.getBinary()
            case 'A':
                message = itch.getBinary(35);
                // Handle message of type A
                break;

            case 'F':
                message = itch.getBinary(39);
                // Handle message of type F
                break;

            case 'E':
                message = itch.getBinary(30);
                // Handle message of type E
                break;

            case 'C':
                message = itch.getBinary(35);
                // Handle message of type C
                break;

            case 'X':
                message = itch.getBinary(22);
                // Handle message of type X
                break;

            case 'D':
                message = itch.getBinary(18);
                // Handle message of type D
                break;

            case 'U':
                message = itch.getBinary(34);
                // Handle message of type U
                break;

            case 'P':
                message = itch.getBinary(43);
                itch.getVWAP(message);
                // Handle message of type P
                break;

            case 'Q':
                message = itch.getBinary(39);
                // Handle message of type Q
                break;

            case 'B':
                message = itch.getBinary(18);
                // Handle message of type B
                break;

            case 'I':
                message = itch.getBinary(49);
                // Handle message of type I
                break;

            case 'N':
                message = itch.getBinary(19);
                // Handle message of type N
                break;

            default:
                console.error('Unknown message header:', msgHeader);
                break;
        }
    }
});

binData.on('end', () => {
    console.log('Processing complete.');
});

binData.on('error', (err) => {
    console.error('Error reading binary data:', err);
});
