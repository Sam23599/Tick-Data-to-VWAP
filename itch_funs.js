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


const support_funs = {
    convertTime(stamp) {
        const time = new Date(stamp / 1e6);
        const formattedTime = time.toISOString().substr(11, 8);
        return formattedTime;
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
    system_event_message: async () => {

    },

    stock_directory: async (msg) => {
        // Stock directory message processing
        const result = struct.unpack('!HH6s8sccIcc2scccccIc', Buffer.from(msg, 'hex'));
        const val = [...result];

        // Implement the rest of your stock_directory function using the 'val' variable
        // For example:
        if (val.length === 38) {
            console.log("Stock Directory Message:", val);
            // Further processing or return as needed
        } else {
            console.log("Invalid Stock Directory Message");
            // Handle invalid message case
        }
    },

    stock_trading_action: async () => {

    },

    short_sale_price_test: async () => {

    },

    market_participation_position: async () => {

    },

    mwcb_decline_level_message: async () => {

    },

    mwcb_status_message: async () => {

    },

    ipo_quoting_period_update: async () => {

    },

    LULD_Auction_Collar: async () => {

    },

    Operational_Halt: async () => {

    },

    add_order_message: async () => {

    },

    add_order_with_mpid: async () => {

    },

    order_executed_message: async () => {

    },

    order_executed_price_message: async () => {

    },

    order_cancel_message: async () => {

    },

    order_delete_message: async () => {

    },

    order_replace_message: async () => {

    },

    trade_message: async () => {

    },

    cross_trade_message: async () => {

    },

    broken_trade_execution_message: async () => {

    },

    net_order_imbalance_message: async () => {

    },

    retail_price_improvement: async () => {

    },

    capital_raise_price_discovery: async () => {

    }
}

module.exports = { itch_fun, support_funs };