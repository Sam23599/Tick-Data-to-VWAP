const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const dfd = require('danfojs-node');
const struct = require('python-struct');
const {itch_fun, support_funs} = require('./itch_funs');
const { exit } = require('process');



// Replace with the path to your data file
const filePath = path.join(__dirname, 'tick-data-file', '01302019.NASDAQ_ITCH50.gz');

// Create a readable stream from the gzipped file
const binData = fs.createReadStream(filePath).pipe(zlib.createGunzip());

binData.on('readable', async () => {
    let msgHeader;
    while ((msgHeader = binData.read(1)) !== null) {
        let message;
        let bin_msg;
        console.log('header ', msgHeader.toString());

        switch (msgHeader.toString()) {
            case 'S':
                // Handle message of type S
                message = binData.read(11);
                bin_msg = await itch_fun.system_event_message(message);
                // console.log(bin_msg);
                break;

            case 'R':
                message = binData.read(38);
                bin_msg = await itch_fun.stock_directory(message);
                // console.log(bin_msg);
                // Handle message of type R
                break;

            case 'H':
                message = binData.read(24);
                bin_msg = await itch_fun.stock_trading_action(message);
                console.log(bin_msg);
                // Handle message of type H
                break;

            case 'Y':
                message = binData.read(19);
                bin_msg = await itch_fun.short_sale_price_test(message);
                console.log(bin_msg);
                // Handle message of type Y
                break;

            case 'L':
                message = binData.read(25);
                bin_msg = await itch_fun.market_participation_position(message);
                console.log(bin_msg);
                // Handle message of type L
                break;

            case 'V':
                message = binData.read(34);
                bin_msg = await itch_fun.mwcb_decline_level_message(message);
                console.log(bin_msg);
                // Handle message of type V
                break;

            case 'W':
                message = binData.read(11);
                bin_msg = await itch_fun.mwcb_status_message(message);
                console.log(bin_msg);
                // Handle message of type W
                break;

            case 'K':
                message = binData.read(27);
                bin_msg = await itch_fun.ipo_quoting_period_update(message);
                console.log(bin_msg);
                // Handle message of type K
                break;

            case 'J':
                message = binData.read(34);
                bin_msg = await itch_fun.LULD_Auction_Collar(message);
                console.log(bin_msg);
                // Handle message of type J
                break;

            case 'h':
                message = binData.read(20);
                bin_msg = await itch_fun.Operational_Halt(message);
                console.log(bin_msg);
                // Handle message of type h 
                break;

            case 'A':
                message = binData.read(35);
                bin_msg = await itch_fun.add_order_message(message);
                console.log(bin_msg);
                // Handle message of type A
                break;

            case 'F':
                message = binData.read(39);
                bin_msg = await itch_fun.add_order_with_mpid(message);
                console.log(bin_msg);
                // Handle message of type F
                break;

            case 'E':
                message = binData.read(30);
                bin_msg = await itch_fun.order_executed_message(message);
                console.log(bin_msg);
                // Handle message of type E
                break;

            case 'C':
                message = binData.read(35);
                bin_msg = await itch_fun.order_executed_price_message(message);
                console.log(bin_msg);
                // Handle message of type C
                break;

            case 'X':
                message = binData.read(22);
                bin_msg = await itch_fun.order_cancel_message(message);
                console.log(bin_msg);
                // Handle message of type X
                break;

            case 'D':
                message = binData.read(18);
                bin_msg = await itch_fun.order_delete_message(message);
                console.log(bin_msg);
                // Handle message of type D
                break;

            case 'U':
                message = binData.read(34);
                bin_msg = await itch_fun.order_replace_message(message);
                console.log(bin_msg);
                // Handle message of type U
                break;

            case 'P':
                message = binData.read(43);
                bin_msg = await itch_fun.trade_message(message);
                console.log(message)
                console.log(bin_msg);

                support_funs.getVWAP(message);
                exit;
                // Handle message of type P
                break;

            case 'Q':
                message = binData.read(39);
                bin_msg = await itch_fun.cross_trade_message(message);
                console.log(bin_msg);
                // Handle message of type Q
                break;

            case 'B':
                message = binData.read(18);
                bin_msg = await itch_fun.broken_trade_execution_message(message);
                console.log(bin_msg);
                // Handle message of type B
                break;

            case 'I':
                message = binData.read(49);
                bin_msg = await itch_fun.net_order_imbalance_message(message);
                console.log(bin_msg);
                // Handle message of type I
                break;

            case 'N':
                message = binData.read(19);
                bin_msg = await itch_fun.retail_price_improvement(message);
                console.log(bin_msg);
                // Handle message of type N
                break;

            case 'O':
                message = binData.read(47);
                bin_msg = await itch_fun.capital_raise_price_discovery(message);
                console.log(bin_msg);
                // Handle message of type O
                break;

            default:
                // console.error('Unknown message header:', msgHeader);
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
