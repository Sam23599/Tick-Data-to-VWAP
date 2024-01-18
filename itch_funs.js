const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const dfd = require('danfojs-node');
const struct = require('python-struct');


this.temp = [];
this.flag = null;

// Creating the 'output' directory if it doesn't exist
const outputPath = path.join('.', 'output');
if (!fs.existsSync(outputPath)) {
    fs.mkdirSync(outputPath);
}

const processResult = (resulted_val) => {
    // Handled the timestamp
    const time_stamp = resulted_val[2];
    resulted_val[2] = support_funs.convertTime(time_stamp);

    // Handled the LongFields
    resulted_val = support_funs.handleLongFields(resulted_val);

    return resulted_val;
}

const support_funs = {

    convertTime(time_stamp) {
        if (!time_stamp) {
            return '00:00:00';
        }
        const binaryArray = new TextEncoder().encode(time_stamp);
        // const nanoseconds = BigInt('0x' + Array.from(binaryArray).map(byte => byte.toString(16).padStart(2, '0')).join(''));
        const nanoseconds = BigInt(parseInt(Array.from(binaryArray).map(byte => byte.toString(2).padStart(8, '0')).join(''), 2));

        const seconds = Number(nanoseconds) / 1e9;
        const date = new Date(seconds * 1000);
        const formattedTime = date.toString().substring(16, 24);
        return formattedTime;
    },

    handleLongFields(resultArray) {
        // Function to convert Long {} to decimal
        const convertLongToDecimal = (longObject) => {
            return longObject.low + longObject.high * 2 ** 32;
        };

        let val = [...resultArray];

        // Find and convert Long {} fields in val
        for (let i = 0; i < val.length; i++) {
            if (typeof val[i] === 'object' && val[i].hasOwnProperty('low') && val[i].hasOwnProperty('high') && val[i].hasOwnProperty('unsigned')) {
                val[i] = convertLongToDecimal(val[i]);
            }
        }

        return val;
    },

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
    },

    getVWAP(message) {
        console.log('message: ', message)
        // const { parsedData, hour } = this.tradeMessage(message);
        const tradeMessageResult = this.tradeMessage(message);

        if (!tradeMessageResult || !Array.isArray(tradeMessageResult.parsedData)) {
            console.error('Error in tradeMessage. Check its implementation.');
            return;
        }

        const { parsedData, hour } = tradeMessageResult;

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
    },

    tradeMessage(msg) {
        if (msg == null) {
            return [];
        }
        const msgType = 'P';
        const temp = struct.unpack('>4s6sQcI8cIQ', msg);
        console.log('temp P: ', temp)
        const newMsg = struct.pack('>s4s2s6sQsI8sIQ', msgType, temp[0], '\x00\x00', temp[1], temp[2], temp[3], temp[4],
            temp.slice(5, 13).join(''), temp[13], temp[14]);
        let value = struct.unpack('>sHHQQsI8sIQ', newMsg);
        console.log('value : ', newMsg)
        value = [...value];
        value[3] = this.convertTime(value[3]);
        value[7] = value[7].trim();
        value[8] = parseFloat(value[8]);
        value[8] /= 10000;
        return [value[3], value[7], value[8], value[6]], value[3].split(':')[0];
    }

}

const itch_fun = {

    // Handle message of type S
    system_event_message: async (msg) => {
        if (msg == null) {
            return [];
        }

        const result = struct.unpack('!HH6sc', msg);
        let val = [...result];

        val = processResult(val);
        if (val.length === 4) {
            // console.log("Stock Directory Message:", val);
            return val;
        } else {
            console.log("Invalid Stock Directory Message");
            return [];
        }
    },

    // Handle message of type R
    stock_directory: async (msg) => {
        if (msg == null) {
            return [];
        }

        const result = struct.unpack('!HH6s8sccIcc2scccccIc', msg);
        let val = [...result];

        val = processResult(val);
        if (val.length === 17) {
            // console.log("Stock Directory Message:", val);
            return val;
        } else {
            console.log("Invalid Stock Directory Message");
            return [];
        }
    },

    // Handle message of type H
    stock_trading_action: async (msg) => {
        if (msg == null) {
            return [];
        }

        const result = struct.unpack('!HH6s8scc4s', msg);
        let val = [...result];

        val = processResult(val);
        if (val.length === 7) {
            // console.log("Stock Directory Message:", val);
            return val;
        } else {
            console.log("Invalid Stock Directory Message");
            return [];
        }
    },

    // Handle message of type Y
    short_sale_price_test: async (msg) => {
        if (msg == null) {
            return [];
        }

        const result = struct.unpack('!HH6s8sc', msg);
        let val = [...result];

        val = processResult(val);
        if (val.length === 5) {
            // console.log("Stock Directory Message:", val);
            return val;
        } else {
            console.log("Invalid Stock Directory Message");
            return [];
        }
    },

    // Handle message of type L
    market_participation_position: async (msg) => {
        if (msg == null) {
            return [];
        }

        const result = struct.unpack('!HH6s4s8sccc', msg);
        let val = [...result];

        val = processResult(val);
        if (val.length === 8) {
            // console.log("Stock Directory Message:", val);
            return val;
        } else {
            console.log("Invalid Stock Directory Message");
            return [];
        }
    },

    // Handle message of type V
    mwcb_decline_level_message: async (msg) => {
        if (msg == null) {
            return [];
        }

        const result = struct.unpack('!HH6sQQQ', msg);
        let val = [...result];

        val = processResult(val);
        if (val.length === 6) {
            // console.log("Stock Directory Message:", val);
            return val;
        } else {
            console.log("Invalid Stock Directory Message");
            return [];
        }
    },

    // Handle message of type W
    mwcb_status_message: async (msg) => {
        if (msg == null) {
            return [];
        }

        const result = struct.unpack('!HH6sc', msg);
        let val = [...result];

        val = processResult(val);
        if (val.length === 4) {
            // console.log("Stock Directory Message:", val);
            return val;
        } else {
            console.log("Invalid Stock Directory Message");
            return [];
        }
    },

    // Handle message of type K
    ipo_quoting_period_update: async (msg) => {
        if (msg == null) {
            return [];
        }

        const result = struct.unpack('!HH6s8sIcL', msg);
        let val = [...result];

        val = processResult(val);
        if (val.length === 7) {
            // console.log("Stock Directory Message:", val);
            return val;
        } else {
            console.log("Invalid Stock Directory Message");
            return [];
        }
    },

    // Handle message of type J
    LULD_Auction_Collar: async (msg) => {
        if (msg == null) {
            return [];
        }

        const result = struct.unpack('!HH6s8sLLLI', msg);
        let val = [...result];

        val = processResult(val);
        if (val.length === 8) {
            // console.log("Stock Directory Message:", val);
            return val;
        } else {
            console.log("Invalid Stock Directory Message");
            return [];
        }
    },

    // Handle message of type h
    Operational_Halt: async (msg) => {
        if (msg == null) {
            return [];
        }

        const result = struct.unpack('!HH6s8scc', msg);
        let val = [...result];

        val = processResult(val);
        if (val.length === 6) {
            // console.log("Stock Directory Message:", val);
            return val;
        } else {
            console.log("Invalid Stock Directory Message");
            return [];
        }
    },

    // Handle message of type A
    add_order_message: async (msg) => {
        if (msg == null) {
            return [];
        }

        const result = struct.unpack('!HH6sQcI8sL', msg);
        let val = [...result];

        val = processResult(val);
        val = processResult(val);

        if (val.length === 8) {
            // console.log("Stock Directory Message:", val);
            return val;
        } else {
            console.log("Invalid Stock Directory Message");
            return [];
        }
    },

    // Handle message of type F
    add_order_with_mpid: async (msg) => {
        if (msg == null) {
            return [];
        }

        const result = struct.unpack('!HH6sQcI8sL4s', msg);
        let val = [...result];

        val = processResult(val);
        if (val.length === 9) {
            // console.log("Stock Directory Message:", val);
            return val;
        } else {
            console.log("Invalid Stock Directory Message");
            return [];
        }
    },

    // Handle message of type E
    order_executed_message: async (msg) => {
        if (msg == null) {
            return [];
        }

        const result = struct.unpack('!HH6sQIQ', msg);
        let val = [...result];

        val = processResult(val);
        if (val.length === 6) {
            // console.log("Stock Directory Message:", val);
            return val;
        } else {
            console.log("Invalid Stock Directory Message");
            return [];
        }
    },

    // Handle message of type C
    order_executed_price_message: async (msg) => {
        if (msg == null) {
            return [];
        }

        const result = struct.unpack('!HH6sQIQcL', msg);
        let val = [...result];

        val = processResult(val);
        if (val.length === 8) {
            // console.log("Stock Directory Message:", val);
            return val;
        } else {
            console.log("Invalid Stock Directory Message");
            return [];
        }
    },

    // Handle message of type X
    order_cancel_message: async (msg) => {
        if (msg == null) {
            return [];
        }

        const result = struct.unpack('!HH6sQI', msg);
        let val = [...result];

        val = processResult(val);
        if (val.length === 5) {
            // console.log("Stock Directory Message:", val);
            return val;
        } else {
            console.log("Invalid Stock Directory Message");
            return [];
        }
    },

    // Handle message of type D
    order_delete_message: async (msg) => {
        if (msg == null) {
            return [];
        }

        const result = struct.unpack('!HH6sQ', msg);
        let val = [...result];

        val = processResult(val);
        if (val.length === 4) {
            // console.log("Stock Directory Message:", val);
            return val;
        } else {
            console.log("Invalid Stock Directory Message");
            return [];
        }
    },

    // Handle message of type U
    order_replace_message: async (msg) => {
        if (msg == null) {
            return [];
        }

        const result = struct.unpack('!HH6sQQIL', msg);
        let val = [...result];

        val = processResult(val);
        if (val.length === 7) {
            // console.log("Stock Directory Message:", val);
            return val;
        } else {
            console.log("Invalid Stock Directory Message");
            return [];
        }
    },

    // Handle message of type P
    trade_message: async (msg) => {
        if (msg == null) {
            return [];
        }
        
        const result = struct.unpack('!HH6sQcI8sLQ', msg);
        let val = [...result];
        console.log('res val: ', val);

        // console.log(val);
        val = processResult(val);

        // remove padded spaces from stock's name
        val[6] = val[6].trim();

        // converted for precision according to given-manual
        val[7] = val[7] / 10000;

        // timestamp, shares, stock-name, price
        let final_result = [val[2], val[5], val[6], val[7]];
        console.log(final_result);
        if (val.length === 9) {
            // console.log("Stock Directory Message:", val);
            return val;
        } else {
            console.log("Invalid Stock Directory Message");
            return [];
        }
    },

    // Handle message of type Q
    cross_trade_message: async (msg) => {
        if (msg == null) {
            return [];
        }

        const result = struct.unpack('!HH6sQ8sLQc', msg);
        let val = [...result];

        val = processResult(val);
        if (val.length === 8) {
            // console.log("Stock Directory Message:", val);
            return val;
        } else {
            console.log("Invalid Stock Directory Message");
            return [];
        }
    },

    // Handle message of type B
    broken_trade_execution_message: async (msg) => {
        if (msg == null) {
            return [];
        }

        const result = struct.unpack('!HH6sQ', msg);
        let val = [...result];

        val = processResult(val);
        if (val.length === 4) {
            // console.log("Stock Directory Message:", val);
            return val;
        } else {
            console.log("Invalid Stock Directory Message");
            return [];
        }
    },

    // Handle message of type I
    net_order_imbalance_message: async (msg) => {
        if (msg == null) {
            return [];
        }

        const result = struct.unpack('!HH6sQQc8sLLLcc', msg);
        let val = [...result];

        val = processResult(val);
        if (val.length === 12) {
            // console.log("Stock Directory Message:", val);
            return val;
        } else {
            console.log("Invalid Stock Directory Message");
            return [];
        }
    },

    // Handle message of type N
    retail_price_improvement: async (msg) => {
        if (msg == null) {
            return [];
        }

        const result = struct.unpack('!HH6s8sc', msg);
        let val = [...result];

        val = processResult(val);
        if (val.length === 5) {
            // console.log("Stock Directory Message:", val);
            return val;
        } else {
            console.log("Invalid Stock Directory Message");
            return [];
        }
    },

    // Handle message of type O
    capital_raise_price_discovery: async (msg) => {
        if (msg == null) {
            return [];
        }

        const result = struct.unpack('!HH6s8scLLL8sLL', msg);
        let val = [...result];

        val = processResult(val);
        if (val.length === 11) {
            // console.log("Stock Directory Message:", val);
            return val;
        } else {
            console.log("Invalid Stock Directory Message");
            return [];
        }
    }
}

module.exports = { itch_fun, support_funs };