`iatool` is my personal quantitative investment analysis tool.

I use it for analysing and screening securities to help me design my portfolio and make sound investment decisions.

The tool features various quantitative tools, metrics and models for various types of analysis, including fundamental analysis, technical analysis, risk analysis, macroeconomic analysis, option pricing, and so on. It covers various financial instruments, including equity instruments (e.g. stocks), fixed-income instruments (e.g. bonds), derivatives (e.g. options), ETFs, commodities, and so on.

The tool employs both an API and a CLI for the user to interface with. It also employs comprehensive documentation for the sake of both accessibility and transparency. Every function implemented in the tool comes with an explicit reference and mathematical description.

In order to use the tool, you must have an API key from [financialmodelingprep.com](). Out of all the data sources I have explored, I have found this API to be the most comprehensive while being affordable.

#### Configuration

In order for the program to work, you must create a configuration file named `config.json` that upholds the following structure:

```json
{
    "api": {
        "fmp": {
            "base": "https://financialmodelingprep.com/api/v3",
            "key": "<your api key>",
            "retry_delay": 5
        }
    }
}
```
