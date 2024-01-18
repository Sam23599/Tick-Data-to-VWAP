# NASDAQ-ITCH5-TICKS to VWAP 

This Node.js project parses trade data from a NASDAQ ITCH 5.0 data feed and calculates the Volume Weighted Average Price (VWAP) for all stocks at every hour, including market close.

## NASDAQ ITCH 5 Data

The NASDAQ ITCH 5.0 data used in this project can be obtained from the following link:

[NASDAQ ITCH 5.0 Data](https://emi.nasdaq.com/ITCH/Nasdaq%20ITCH/01302019.NASDAQ_ITCH50.gz)

Please note that the link may expire after some days, so ensure to change the date in the filename to the trading weekday.

The data format is defined by the NASDAQ ITCH 5.0 specification, which can be found in the following document:

[NASDAQ ITCH 5.0 Specification](https://www.nasdaqtrader.com/content/technicalsupport/specifications/dataproducts/NQTVITCHspecification.pdf)

## Volume Weighted Average Price for ITCH Ticks

The Volume Weighted Average Price (VWAP) is calculated based on the ITCH ticks data. For a detailed explanation of VWAP, refer to the following resources:

- [StockCharts - VWAP for Tick Data](http://stockcharts.com/school/doku.php?id=chart_school:technical_indicators:vwap_tick_data)

- [Investopedia - VWAP (Volume Weighted Average Price)](http://www.investopedia.com/terms/v/vwap.asp)

## Installation

1. Ensure you have Node.js installed on your machine. You can download it from [https://nodejs.org/](https://nodejs.org/).

2. Clone this repository to your local machine.

   ```bash
   git clone https://github.com/your-username/your-repository.git

3. Navigate to the project directory.

    ```bash
    cd your-repository

4. Install the required dependencies.

    ```bash
    npm install

## Usage 

1. Update the current_VWAP object in the index.js file with your specific data.

2. Run the script to generate VWAP files.

    ```bash
    node index.js

This will create CSV files in the ./output/ folder, each containing VWAP data for a specific stock and hour.

## File Structure

- index.js: Main script for parsing NASDAQ ITCH 5.0 data and calculating VWAP.
- ./output/: Folder containing the generated CSV files.

## Configuration
Adjust the following variables in index.js as needed:

## License
This project is licensed under the MIT License.

Feel free to modify this README.md to better suit your project's details and structure. If you have any questions or need further assistance, don't hesitate to reach out.

    ```csharp
    Replace "your-username" and "your-repository" with your actual GitHub username and repository name. Adjust the instructions and details based on your specific project requirements.
    ```