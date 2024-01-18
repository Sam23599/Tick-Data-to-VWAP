const fs = require('fs');
const path = require('path');
const struct = require('python-struct');


let current_VWAP = {};
const VWAP_funcs = {
    getCurrentVWAP() {
        return current_VWAP
    },

    createVWAPFiles(current_VWAP) {
        const uploadFolderPath = './output/';
        fs.mkdirSync(uploadFolderPath, { recursive: true });  // Ensure the folder exists

        Object.keys(current_VWAP).forEach((key) => {
            if (key.match(/_\d{2}$/)) {
                const hour = key.split('_')[1];
                const fileName = path.join(uploadFolderPath, `vwap_${hour}.csv`);
    
                const data = current_VWAP[key];
                const vwap = current_VWAP[`${data.stock}_VWAP`];
    
                // Use 'a' flag to append to the file if it exists
                const csvContent = `time,stock name,stock vwap\n${data.timestamp},${data.stock},${vwap}\n`;
                fs.writeFileSync(fileName, csvContent, { flag: 'a' });
            }
        });
    }

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

    calculateVWAP(currentVWAP, timestamp, stock, shares, price) {
        const currentHour = timestamp.slice(0, 2);
        const vwapKey = `${stock}_${currentHour}`;

        if (!currentVWAP[vwapKey]) {
            currentVWAP[vwapKey] = {
                timestamp: timestamp,
                stock: stock,
                totalVolume: 0,
                cumulativePriceVolume: 0,
            };
        }

        const vwapInfo = currentVWAP[vwapKey];
        vwapInfo.totalVolume += shares;
        vwapInfo.cumulativePriceVolume += shares * price;

        const vwap = vwapInfo.totalVolume !== 0
            ? vwapInfo.cumulativePriceVolume / vwapInfo.totalVolume
            : 0;

        return {
            ...currentVWAP,
            [vwapKey]: vwapInfo,
            [`${stock}_VWAP`]: vwap.toFixed(4),
        };
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

        val = processResult(val);

        // remove padded spaces from stock's name
        val[6] = val[6].replace(/\s/g, '').trim();

        // converted for price precision as per given-manual
        val[7] = val[7] / 10000;

        current_VWAP = support_funs.calculateVWAP(current_VWAP, val[2], val[6], val[5], val[7]);

        if (val.length === 9) {
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
            return val;
        } else {
            console.log("Invalid Stock Directory Message");
            return [];
        }
    }
}

module.exports = { itch_fun, VWAP_funcs };